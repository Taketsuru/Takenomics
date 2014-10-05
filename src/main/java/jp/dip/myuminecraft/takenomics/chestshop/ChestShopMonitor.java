package jp.dip.myuminecraft.takenomics.chestshop;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import jp.dip.myuminecraft.takenomics.Constants;
import jp.dip.myuminecraft.takenomics.Database;
import jp.dip.myuminecraft.takenomics.Logger;
import jp.dip.myuminecraft.takenomics.SignScanEvent;
import jp.dip.myuminecraft.takenomics.UnknownPlayerException;
import jp.dip.myuminecraft.takenomics.models.PlayerTable;
import jp.dip.myuminecraft.takenomics.models.WorldTable;

import org.bukkit.Chunk;
import org.bukkit.Material;
import org.bukkit.Server;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.block.BlockFace;
import org.bukkit.block.BlockState;
import org.bukkit.block.Chest;
import org.bukkit.block.DoubleChest;
import org.bukkit.block.Sign;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.inventory.InventoryCloseEvent;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.InventoryHolder;
import org.bukkit.inventory.ItemStack;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.java.JavaPlugin;

import com.Acrobot.Breeze.Utils.MaterialUtil;
import com.Acrobot.Breeze.Utils.PriceUtil;
import com.Acrobot.ChestShop.ChestShop;
import com.Acrobot.ChestShop.Events.ShopCreatedEvent;
import com.Acrobot.ChestShop.Events.ShopDestroyedEvent;
import com.Acrobot.ChestShop.Events.TransactionEvent;
import com.Acrobot.ChestShop.Events.TransactionEvent.TransactionType;
import com.Acrobot.ChestShop.Signs.ChestShopSign;
import com.Acrobot.ChestShop.UUIDs.NameManager;
import com.Acrobot.ChestShop.Utils.uBlock;

public class ChestShopMonitor implements Listener {

    class ShopRecord {
        int    x;
        int    z;
        int    y;
        String world;
        UUID   owner;
        int    quantity;
        String material;
        double buyPrice;
        double sellPrice;
        int    stock;
        int    purchasableQuantity;

        ShopRecord(Sign sign, String[] lines)
                throws UnknownPlayerException, SQLException {
            x = sign.getX();
            z = sign.getZ();
            y = sign.getY();
            world = sign.getWorld().getName();

            if (ChestShopSign.isAdminShop(sign)) {
                owner = null;
            } else {
                String nameOnSign = lines[ChestShopSign.NAME_LINE];
                String ownerName = NameManager.getFullUsername(nameOnSign);
                owner = NameManager.getUUID(ownerName);
            }

            quantity = Integer.parseInt(lines[ChestShopSign.QUANTITY_LINE]);

            String prices = lines[ChestShopSign.PRICE_LINE];
            buyPrice = PriceUtil.getBuyPrice(prices);
            sellPrice = PriceUtil.getSellPrice(prices);

            if (ChestShopSign.isAdminShop(sign)) {
                stock = -1;
                purchasableQuantity = -1;
            } else {
                stock = 0;
                purchasableQuantity = 0;

                Chest chest = uBlock.findConnectedChest(sign);
                if (chest != null) {
                    Inventory inventory = chest.getInventory();
                    material = lines[ChestShopSign.ITEM_LINE];
                    ItemStack item = MaterialUtil.getItem(material);
                    if (item != null) {
                        int slotCount = inventory.getSize();
                        for (int i = 0; i < slotCount; ++i) {
                            ItemStack slot = inventory.getItem(i);
                            if (slot == null || slot.getType().equals(Material.AIR)) {
                                purchasableQuantity += item.getMaxStackSize();
                            } else if (slot.isSimilar(item)) {
                                purchasableQuantity += item.getMaxStackSize() - slot.getAmount();
                                stock += slot.getAmount();
                            }
                        }
                    }
                }
            }
        }
    }

    public static final int currentSchemaVersion = 1;
    public static final int maxBatchSize         = 100;
    JavaPlugin              plugin;
    Logger                  logger;
    Database                database;
    PlayerTable             playerTable;
    WorldTable              worldTable;
    String                  shopTableName;
    String                  transactionTableName;
    String                  temporaryShopTableName;
    PreparedStatement       truncateTemporary;
    PreparedStatement       insertIntoTemporary;
    PreparedStatement       deleteObsoleteShopsInXZRange;
    PreparedStatement       deleteObsoleteShops;
    PreparedStatement       replaceShops;
    PreparedStatement       insertTransaction;
    PreparedStatement       getShopIdAt;
    PreparedStatement       deleteShop;
    PreparedStatement       scrubShops;
    boolean                 needScrubing;

    public ChestShopMonitor(JavaPlugin plugin, Logger logger,
            Database database, PlayerTable playerTable, WorldTable worldTable) {
        this.plugin = plugin;
        this.logger = logger;
        this.database = database;
        this.playerTable = playerTable;
        this.worldTable = worldTable;
    }

    public boolean enable() {
        if (database == null || playerTable == null || worldTable == null) {
            logger.warning("No database connection.  Disabled chestshop monitor.");
            return false;
        }
        
        Plugin result = plugin.getServer().getPluginManager().getPlugin("ChestShop");
        if (result == null || ! (result instanceof ChestShop)) {
            logger.warning("ChestShop is not found.");
            return false;
        }

        String tablePrefix = database.getTablePrefix();
        shopTableName = tablePrefix + "shops";
        transactionTableName = tablePrefix + "transactions";
        temporaryShopTableName = tablePrefix + "shops_temp";

        try {
            needScrubing = false;
            if (setupShopTable()) {
                return false;
            }
            createTransactionTableIfNecessary();
            createTemporaryShopTableIfNecessary();
            prepareStatements();
            truncateTemporary.executeUpdate();
            if (needScrubing) {
                needScrubing = false;
                scrub();
            }
        } catch (SQLException e) {
            logger.warning(e, "Failed to initialize ChestShop monitor.");
            disable();
            return false;
        }

        plugin.getServer().getPluginManager().registerEvents(this, plugin);
        return true;
    }

    boolean setupShopTable() throws SQLException {
        if (! database.hasTable(shopTableName)) {
            createShopTable(shopTableName);
            return false;
        }
        
        int existingTableVersion = database.getVersion(shopTableName);  
        switch (existingTableVersion) {  

        case currentSchemaVersion:
            break;

        case 0:
            upgradeShopTableFromVersion0();
            break;
        
        default:
            logger.warning("%s: Unsupported table schema version %d",
                    shopTableName, existingTableVersion);
            return true;
        }

        return false;
    }

    void createShopTable(String tableName) throws SQLException {
        Connection connection = database.getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format
                    ("CREATE TABLE %s ("
                            + "id SMALLINT UNSIGNED AUTO_INCREMENT NOT NULL,"
                            + "x SMALLINT NOT NULL,"
                            + "z SMALLINT NOT NULL,"
                            + "y SMALLINT NOT NULL,"
                            + "world TINYINT UNSIGNED NOT NULL,"
                            + "owner MEDIUMINT UNSIGNED,"
                            + "quantity SMALLINT UNSIGNED NOT NULL,"
                            + "material VARCHAR(16) CHARACTER SET ascii NOT NULL,"
                            + "buy_price DOUBLE,"
                            + "sell_price DOUBLE,"
                            + "stock SMALLINT UNSIGNED,"
                            + "purchasable_quantity SMALLINT UNSIGNED,"
                            + "PRIMARY KEY (id),"
                            + "UNIQUE (x,z,y,world),"
                            + "FOREIGN KEY (world) REFERENCES %s (id),"
                            + "FOREIGN KEY (owner) REFERENCES %s (id)"
                            + ")",
                            shopTableName, worldTable.getTableName(), playerTable.getTableName()));
        }
        database.setVersion(shopTableName, currentSchemaVersion);
    }

    void upgradeShopTableFromVersion0() throws SQLException {
        Connection connection = database.getConnection();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format("ALTER TABLE %s ADD COLUMN stock SMALLINT UNSIGNED, "
                    + "ADD COLUMN purchasable_quantity SMALLINT UNSIGNED,"
                    + "DROP INDEX owner",
                    shopTableName));
        }
        database.setVersion(shopTableName, currentSchemaVersion);
        needScrubing = true;
    }

    void scrub() throws SQLException {
        Server server = plugin.getServer();
        Connection connection = database.getConnection();

        try (Statement stmt = connection.createStatement()) {

            connection.setAutoCommit(false);

            int batchSize = 0;
            try (ResultSet rs = stmt.executeQuery
                (String.format("SELECT x,z,y,t2.name "
                        + "FROM %s t1 INNER JOIN %s t2 ON t1.world = t2.id",
                        shopTableName, worldTable.getTableName()))) {
                while (rs.next()) {
                    int x = rs.getInt(1);
                    int z = rs.getInt(2);
                    int y = rs.getInt(3);
                    String worldName = rs.getString(4);
                    World world = server.getWorld(worldName);

                    int chunkX = x / Constants.chunkXSize;
                    int chunkZ = z / Constants.chunkZSize;
                    if (! world.isChunkLoaded(chunkX, chunkZ)) {
                        world.loadChunk(chunkX, chunkZ);
                    }
                    
                    Block block = world.getBlockAt(x, y, z);
                    BlockState state = block.getState();
                    if (! (state instanceof Sign)) {
                        continue;
                    }
                    Sign sign = (Sign)state;
                    
                    if (! ChestShopSign.isValid(sign)) {
                        continue;
                    }
                    
                    try {
                        ShopRecord shop = new ShopRecord(sign, sign.getLines());
                        setInsertShopParams(shop, insertIntoTemporary);
                        insertIntoTemporary.addBatch();
                        if (maxBatchSize <= ++batchSize) {
                            insertIntoTemporary.executeBatch();
                            batchSize = 0;
                        }
                    } catch (UnknownPlayerException e) {
                    }
                }
                insertIntoTemporary.executeBatch();

                scrubShops.executeUpdate();
                replaceShops.executeUpdate();
                truncateTemporary.executeUpdate();   
                
                connection.commit();
            }
        } finally {
            connection.setAutoCommit(true);   
        }
    }
    
    void createTransactionTableIfNecessary() throws SQLException {
        Connection connection = database.getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format("CREATE TABLE IF NOT EXISTS %s ("
                    + "id INT UNSIGNED AUTO_INCREMENT NOT NULL,"
                    + "timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,"
                    + "shop SMALLINT UNSIGNED NOT NULL,"
                    + "player MEDIUMINT UNSIGNED NOT NULL,"
                    + "type ENUM('buy', 'sell') NOT NULL,"
                    + "quantity SMALLINT NOT NULL,"
                    + "PRIMARY KEY (id),"
                    + "FOREIGN KEY (shop) REFERENCES %s (id),"
                    + "FOREIGN KEY (player) REFERENCES %s (id)"
                    + ")",
                    transactionTableName, shopTableName, playerTable.getTableName()));
        }
    }

    void createTemporaryShopTableIfNecessary() throws SQLException {
        Connection connection = database.getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format("CREATE TEMPORARY TABLE IF NOT EXISTS %s LIKE %s",
                            temporaryShopTableName, shopTableName));
        }
    }

    void prepareStatements() throws SQLException {
        Connection connection = database.getConnection();

        truncateTemporary = connection.prepareStatement
                (String.format("TRUNCATE %s", temporaryShopTableName));
        deleteObsoleteShopsInXZRange = connection.prepareStatement
                (String.format("DELETE t1 FROM %s t1 LEFT JOIN %s t2 USING (x,z,y,world) "
                        + "WHERE ? <= t1.x AND t1.x < ? "
                        + "AND ? <= t1.z AND t1.z < ? "
                        + "AND (! (t1.owner <=> t2.owner) "
                        + "OR t1.quantity != t2.quantity "
                        + "OR t1.material != t2.material "
                        + "OR ! (t1.buy_price <=> t2.buy_price) "
                        + "OR ! (t1.sell_price <=> t2.sell_price))",
                        shopTableName, temporaryShopTableName));
        deleteObsoleteShops = connection.prepareStatement
                (String.format("DELETE t1 FROM %s t1 INNER JOIN %s t2 USING (x,z,y,world) "
                        + "WHERE ! (t1.owner <=> t2.owner) "
                        + "OR t1.quantity != t2.quantity "
                        + "OR t1.material != t2.material "
                        + "OR ! (t1.buy_price <=> t2.buy_price) "
                        + "OR ! (t1.sell_price <=> t2.sell_price)",
                        shopTableName, temporaryShopTableName));
        scrubShops = connection.prepareStatement
                (String.format("DELETE t1 FROM %s t1 LEFT JOIN %s t2 USING (x,z,y,world) "
                        + "WHERE ! (t1.owner <=> t2.owner) "
                        + "OR t1.quantity != t2.quantity "
                        + "OR t1.material != t2.material "
                        + "OR ! (t1.buy_price <=> t2.buy_price) "
                        + "OR ! (t1.sell_price <=> t2.sell_price)",
                        shopTableName, temporaryShopTableName));
        insertIntoTemporary = connection.prepareStatement
                (String.format("INSERT INTO %s VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?)",
                        temporaryShopTableName));
        replaceShops = connection.prepareStatement
                (String.format("REPLACE %s "
                        + "SELECT t1.id,"
                        + "t2.x,"
                        + "t2.z,"
                        + "t2.y,"
                        + "t2.world,"
                        + "t2.owner,"
                        + "t2.quantity,"
                        + "t2.material,"
                        + "t2.buy_price,"
                        + "t2.sell_price, "
                        + "t2.stock, "
                        + "t2.purchasable_quantity "
                        + "FROM %s t1 RIGHT JOIN %s t2 USING (x,z,y,world) ",
                        shopTableName, shopTableName, temporaryShopTableName));
        getShopIdAt = connection.prepareStatement
                (String.format("SELECT id FROM %s WHERE x=? AND z=? AND y=? AND world=?",
                        shopTableName));
        insertTransaction = connection.prepareStatement
                (String.format("INSERT INTO %s VALUES (NULL,NULL,?,?,?,?)",
                        transactionTableName));
        deleteShop = connection.prepareStatement
                (String.format("DELETE FROM %s WHERE "
                        + "x = ? "
                        + "AND z = ? "
                        + "AND y = ? "
                        + "AND world = ?",
                        shopTableName));
    }

    public void disable() {
        shopTableName = null;
        transactionTableName = null;
        temporaryShopTableName = null;

        if (truncateTemporary != null) {
            try { truncateTemporary.close(); } catch (SQLException e) {}
            truncateTemporary = null;
        }
        if (insertIntoTemporary != null) {
            try { insertIntoTemporary.close(); } catch (SQLException e) {}
            insertIntoTemporary = null;
        }
        if (deleteObsoleteShopsInXZRange != null) {
            try { deleteObsoleteShopsInXZRange.close(); } catch (SQLException e) {}
            deleteObsoleteShopsInXZRange = null;
        }
        if (deleteObsoleteShops != null) {
            try { deleteObsoleteShops.close(); } catch (SQLException e) {}
            deleteObsoleteShops = null;
        }
        if (scrubShops != null) {
            try { scrubShops.close(); } catch (SQLException e) {}
            scrubShops = null;
        }
        if (replaceShops != null) {
            try { replaceShops.close(); } catch (SQLException e) {}
            replaceShops = null;
        }
        if (getShopIdAt != null) {
            try { getShopIdAt.close(); } catch (SQLException e) {}
            getShopIdAt = null;
        }
        if (insertTransaction != null) {
            try { insertTransaction.close(); } catch (SQLException e) {}
            insertTransaction = null;
        }
        if (deleteShop != null) {
            try { deleteShop.close(); } catch (SQLException e) {}
            deleteShop = null;
        }
    }

    @EventHandler
    public void onSignScanEvent(SignScanEvent event) {
        try {
            Chunk chunk = event.getChunk();
            final int chunkX = chunk.getX();
            final int chunkZ = chunk.getZ();
            final List<ShopRecord> signs = new ArrayList<ShopRecord>();
            for (Sign sign : event.getSigns()) {
                if (ChestShopSign.isValid(sign)) {
                    signs.add(new ShopRecord(sign, sign.getLines()));
                }
            }

            database.runAsynchronously(new Runnable() {
                public void run() {
                    syncSigns(chunkX, chunkZ, signs);
                }
            });
        } catch (Exception e) {
            logger.warning(e, "Failed to update scanned signs.");
        }
    }

    void syncSigns(int chunkX, int chunkZ, List<ShopRecord> scannedSigns) {
        Connection connection = database.getConnection();
      
        try {
            try {
                connection.setAutoCommit(false);

                int batchSize = 0;
                for (ShopRecord row : scannedSigns) {
                    setInsertShopParams(row, insertIntoTemporary);
                    insertIntoTemporary.addBatch();
                    if (maxBatchSize <= ++batchSize) {
                        insertIntoTemporary.executeBatch();
                        batchSize = 0;
                    }
                }
                insertIntoTemporary.executeBatch();

                deleteObsoleteShopsInXZRange.setInt(1, chunkX * Constants.chunkXSize);
                deleteObsoleteShopsInXZRange.setInt(2, (chunkX + 1) * Constants.chunkXSize);
                deleteObsoleteShopsInXZRange.setInt(3, chunkZ * Constants.chunkZSize);
                deleteObsoleteShopsInXZRange.setInt(4, (chunkZ + 1) * Constants.chunkZSize);
                deleteObsoleteShopsInXZRange.executeUpdate();
                replaceShops.executeUpdate();
                truncateTemporary.executeUpdate();

                connection.commit();
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            logger.warning(e, "Failed to enter an entry into ChestShop shops table.");            
        }
    }
    
    void setInsertShopParams(ShopRecord row, PreparedStatement stmt) throws SQLException {
        stmt.clearParameters();
        stmt.setInt(1, row.x);
        stmt.setInt(2, row.z);
        stmt.setInt(3, row.y);
        stmt.setInt(4, worldTable.getId(row.world));
        if (row.owner != null) {
            stmt.setInt(5, playerTable.getId(row.owner));
        } else {
            stmt.setNull(5, Types.INTEGER);
        }
        stmt.setInt(6, row.quantity);
        stmt.setString(7, row.material);
        if (0.0 <= row.buyPrice) {
            stmt.setDouble(8, row.buyPrice);
        } else {
            stmt.setNull(8, Types.DOUBLE);
        }
        if (0.0 <= row.sellPrice) {
            stmt.setDouble(9, row.sellPrice);
        } else {
            stmt.setNull(9, Types.DOUBLE);
        }
        if (0 <= row.stock) {
            stmt.setInt(10, row.stock);
        } else {
            stmt.setNull(10, Types.INTEGER);
        }
        if (0 <= row.purchasableQuantity) {
            stmt.setInt(11, row.purchasableQuantity);
        } else {
            stmt.setNull(11, Types.INTEGER);
        }
    }

    @EventHandler
    void onShopCreatedEvent(ShopCreatedEvent event) {
        try {
            final ShopRecord row = new ShopRecord
                    (event.getSign(), event.getSignLines());
            database.runAsynchronously(new Runnable() {
                public void run() {
                    try {
                        Connection connection = database.getConnection();

                        try {
                            connection.setAutoCommit(false);

                            setInsertShopParams(row, insertIntoTemporary);
                            insertIntoTemporary.executeUpdate();
                            deleteObsoleteShops.executeUpdate();
                            replaceShops.executeUpdate();
                            truncateTemporary.executeUpdate();
                            
                            connection.commit();
                        } finally {
                            connection.setAutoCommit(true);
                        }
                    } catch (SQLException e) {
                        logger.warning(e, "Failed to enter a shop record.");
                    }
                }
            });
        } catch (Exception e) {
            logger.warning(e, "Failed to enter shop record.");
        }
    }

    @EventHandler
    void onShopDestroyedEvent(ShopDestroyedEvent event) {
        try {
            final ShopRecord row = new ShopRecord
                    (event.getSign(), event.getSign().getLines());
            database.runAsynchronously(new Runnable() {
                public void run() {
                    try {
                        deleteShop.setInt(1, row.x);
                        deleteShop.setInt(2, row.z);
                        deleteShop.setInt(3, row.y);
                        deleteShop.setInt(4, worldTable.getId(row.world));
                        deleteShop.executeUpdate();
                    } catch (SQLException e) {
                        logger.warning(e, "Failed to delete shop record.");
                    }
                }
            });
        } catch (Exception e) {
            logger.warning(e, "Failed to delete shop record.");
        }
    }

    @EventHandler
    void onTransactionEvent(TransactionEvent event) {
        try {
            final ShopRecord row =
                    new ShopRecord(event.getSign(), event.getSign().getLines());
            final int amount = event.getStock()[0].getAmount();
            final TransactionType type = event.getTransactionType();
            final int playerId = playerTable.getId(event.getClient().getUniqueId());
            database.runAsynchronously(new Runnable() {
                public void run() {
                    try {
                        insertTransactionRecord(row, playerId, type, amount);
                    } catch (SQLException e) {
                        logger.warning(e, "Failed to insert transaction record.");
                    }
                }
            });
        } catch (Exception e) {
            logger.warning(e, "Failed to delete shop record.");
        }
    }

    void insertTransactionRecord(ShopRecord row, int playerId,
            TransactionType type, int amount) throws SQLException {
        if (0 <= row.stock) {
            switch (type) {
            case BUY:
                row.stock -= amount;
                row.purchasableQuantity += amount;
                break;                
            case SELL:
                row.stock += amount;
                row.purchasableQuantity -= amount;
                break;
            default:
                assert false : "type = " + type;
            }
        }

        Connection connection = database.getConnection();
        try (Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);

            setInsertShopParams(row, insertIntoTemporary);
            insertIntoTemporary.executeUpdate();
            deleteObsoleteShops.executeUpdate();
            replaceShops.executeUpdate();
            truncateTemporary.executeUpdate();
            
            getShopIdAt.setInt(1, row.x);
            getShopIdAt.setInt(2, row.z);
            getShopIdAt.setInt(3, row.y);
            getShopIdAt.setInt(4, worldTable.getId(row.world));

            int shopId;
            try (ResultSet resultSet = getShopIdAt.executeQuery()) {            
                resultSet.next();
                shopId = resultSet.getInt(1);
            }

            insertTransaction.setInt(1, shopId);
            insertTransaction.setInt(2, playerId);
            insertTransaction.setString(3, type.toString().toLowerCase());
            insertTransaction.setInt(4, amount);
            insertTransaction.executeUpdate();

            connection.commit();
        } finally {
            connection.setAutoCommit(true);
        }
    }
    
    static final BlockFace chestFaces[] = {
        BlockFace.SOUTH,
        BlockFace.WEST,
        BlockFace.NORTH,
        BlockFace.EAST,
        BlockFace.UP,
        BlockFace.DOWN
    };

    @EventHandler
    public void onInventoryClose(InventoryCloseEvent event) {
        InventoryHolder holder = event.getInventory().getHolder();

        Set<Sign> signs = new HashSet<Sign>();
        
        if (holder instanceof Chest) {
            addAttachingChestShopSigns((Chest)holder, signs);
        } else if (holder instanceof DoubleChest) {
            DoubleChest doubleChest = (DoubleChest)holder;
            addAttachingChestShopSigns((Chest)(doubleChest.getLeftSide()), signs);
            addAttachingChestShopSigns((Chest)(doubleChest.getRightSide()), signs);
        }

        if (signs.isEmpty()) {
            return;
        }

        final Connection connection = database.getConnection();

        try {
            final ArrayList<ShopRecord> rows = new ArrayList<ShopRecord>();
            for (Sign sign : signs) {
                rows.add(new ShopRecord(sign, sign.getLines()));
            }

            database.runAsynchronously(new Runnable() {
                public void run() {
                    try {
                        try {
                            connection.setAutoCommit(false);

                            int batchSize = 0;
                            for (ShopRecord row : rows) {
                                setInsertShopParams(row, insertIntoTemporary);
                                insertIntoTemporary.addBatch();
                                if (maxBatchSize <= ++batchSize) {
                                    insertIntoTemporary.executeBatch();
                                    batchSize = 0;
                                }
                            }
                            insertIntoTemporary.executeBatch();                            
                            deleteObsoleteShops.executeUpdate();
                            replaceShops.executeUpdate();
                            truncateTemporary.executeUpdate();

                            connection.commit();
                        } finally {
                            connection.setAutoCommit(true);
                        }
                        
                    } catch (SQLException e) {
                        logger.warning(e, "Failed to enter a shop record.");
                    }
                }
            });
        } catch (Exception e) {
            logger.warning(e, "Failed to enter a shop record.");           
        }
    }

    void addAttachingChestShopSigns(Chest chest, Set<Sign> target) {
        Block chestBlock = chest.getBlock();
        for (BlockFace face : chestFaces) {
            Block signBlock = chestBlock.getRelative(face);
            BlockState state = signBlock.getState();
            if (! (state instanceof Sign)) {
                continue;
            }

            Sign sign = (Sign)state;
            if (! ChestShopSign.isValid(sign)) {
                continue;
            }

            if (! chest.equals(uBlock.findConnectedChest(sign))) {
                continue;
            }

            target.add(sign);
        }
    }

}
