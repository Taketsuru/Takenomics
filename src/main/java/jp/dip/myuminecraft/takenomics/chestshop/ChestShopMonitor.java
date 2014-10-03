package jp.dip.myuminecraft.takenomics.chestshop;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
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

    class ChestshopsRow {
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

        ChestshopsRow(Sign sign, String[] lines)
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
    PreparedStatement       deleteObsoleteShops;
    PreparedStatement       setStockAndPurchasableQuantity;
    PreparedStatement       insertNewShops;
    PreparedStatement       insertTransaction;
    PreparedStatement       selectIdFromShops;
    PreparedStatement       insertShop;
    PreparedStatement       insertShopReturnKey;
    PreparedStatement       updateStock;
    PreparedStatement       deleteShop;

    public ChestShopMonitor(JavaPlugin plugin, Logger logger,
            Database database, PlayerTable playerMonitor, WorldTable worldTable) {
        this.plugin = plugin;
        this.logger = logger;
        this.database = database;
        this.playerTable = playerMonitor;
        this.worldTable = worldTable;
    }

    public boolean enable() {
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
            if (setupShopTable()) {
                return false;
            }
            createTransactionTableIfNecessary();
            createTemporaryShopTableIfNecessary();
            prepareStatements();
            truncateTemporary.executeUpdate();
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
            createEmptyShopTable(shopTableName);
            database.setVersion(shopTableName, currentSchemaVersion);
            return false;
        }
        
        int existingTableVersion = database.getVersion(shopTableName);       
        switch (existingTableVersion) {  

        case currentSchemaVersion:
            return false;

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

    void createEmptyShopTable(String tableName) throws SQLException {
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
                            + "stock INT,"
                            + "purchasable_quantity INT,"
                            + "PRIMARY KEY (id),"
                            + "UNIQUE (x,z,y,world),"
                            + "INDEX (owner),"
                            + "FOREIGN KEY (world) REFERENCES %s (id) ON DELETE CASCADE,"
                            + "FOREIGN KEY (owner) REFERENCES %s (id) ON DELETE CASCADE"
                            + ")",
                            shopTableName, worldTable.getTableName(), playerTable.getTableName()));
        }
    }

    void upgradeShopTableFromVersion0() throws SQLException {
        String newTableName = shopTableName + "_new";
        createEmptyShopTable(newTableName);

        Connection connection = database.getConnection();
        Server server = plugin.getServer();

        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery
                    (String.format("SELECT id, x, z, y, world, "
                            + "owner, quantity, material, "
                            + "buy_price, sell_price FROM %s",
                            shopTableName))) {
                while (rs.next()) {
                    World world = server.getWorld(rs.getString(5));
                    if (world == null) {
                        continue;
                    }

                    int x = rs.getInt(2);
                    int z = rs.getInt(3);
                    int y = rs.getInt(4);
                    if (! world.isChunkLoaded
                            (x / Constants.chunkXSize, z / Constants.chunkZSize)) {
                        world.loadChunk(x, z);
                    }

                    Block block = world.getBlockAt(x, y, z);
                    BlockState state = block.getState();
                    if (state == null || ! (state instanceof Sign)) {
                        continue;
                    }
                    Sign sign = (Sign)state;

                    if (! ChestShopSign.isValid(sign)) {
                        continue;
                    }

                    try {
                        setInsertIntoTemporaryParams(new ChestshopsRow(sign, sign.getLines()), insertShop);
                        insertShop.executeUpdate();
                    } catch (UnknownPlayerException e) {
                    }
                }

                stmt.executeUpdate(String.format
                        ("RENAME TABLE %1$s TO %1$s_save, %2$s TO %1$s", shopTableName, newTableName));
                database.setVersion(shopTableName, currentSchemaVersion);
                stmt.executeUpdate(String.format("DROP TABLE IF EXISTS %s_save", shopTableName));
            }
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
                    + "FOREIGN KEY (shop) REFERENCES %s (id) ON DELETE CASCADE,"
                    + "FOREIGN KEY (player) REFERENCES %s (id) ON DELETE CASCADE"
                    + ")",
                    transactionTableName, shopTableName, playerTable.getTableName()));
        }
    }

    void createTemporaryShopTableIfNecessary() throws SQLException {
        Connection connection = database.getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format("CREATE TEMPORARY TABLE IF NOT EXISTS %s ("
                            + "x SMALLINT NOT NULL,"
                            + "z SMALLINT NOT NULL,"
                            + "y SMALLINT NOT NULL,"
                            + "world SMALLINT UNSIGNED NOT NULL,"
                            + "owner INT UNSIGNED,"
                            + "quantity SMALLINT UNSIGNED NOT NULL,"
                            + "material VARCHAR(16) CHARACTER SET ascii NOT NULL,"
                            + "buy_price DOUBLE,"
                            + "sell_price DOUBLE,"
                            + "stock INT,"
                            + "purchasable_quantity INT,"
                            + "UNIQUE (x,z,y,world)"
                            + ") ENGINE=MEMORY",
                            temporaryShopTableName));
        }
    }

    void prepareStatements() throws SQLException {
        Connection connection = database.getConnection();

        truncateTemporary = connection.prepareStatement
                (String.format("TRUNCATE %s", temporaryShopTableName));
        deleteObsoleteShops = connection.prepareStatement
                (String.format("DELETE FROM %1$s "
                        + "USING %1$s LEFT JOIN %2$s "
                        + "ON %1$s.x = %2$s.x "
                        + "AND %1$s.z = %2$s.z "
                        + "AND %1$s.y = %2$s.y "
                        + "AND %1$s.world = %2$s.world "
                        + "WHERE ? <= %1$s.x AND %1$s.x < ? "
                        + "AND ? <= %1$s.z AND %1$s.z < ? "
                        + "AND (%2$s.x IS NULL "
                        + "OR ! (%1$s.owner <=> %2$s.owner) "
                        + "OR %1$s.quantity != %2$s.quantity "
                        + "OR %1$s.material != %2$s.material "
                        + "OR ! (%1$s.buy_price <=> %2$s.buy_price) "
                        + "OR ! (%1$s.sell_price <=> %2$s.sell_price))",
                        shopTableName, temporaryShopTableName));
        insertIntoTemporary = connection.prepareStatement
                (String.format("INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        temporaryShopTableName));
        insertNewShops = connection.prepareStatement
                (String.format("INSERT INTO %1$s "
                        + "SELECT NULL,"
                        + "%2$s.x,"
                        + "%2$s.z,"
                        + "%2$s.y,"
                        + "%2$s.world,"
                        + "%2$s.owner,"
                        + "%2$s.quantity,"
                        + "%2$s.material,"
                        + "%2$s.buy_price,"
                        + "%2$s.sell_price "
                        + "%2$s.stock "
                        + "%2$s.purchasable_quantity "
                        + "FROM %2$s LEFT JOIN %1$s "
                        + "ON %1$s.x = %2$s.x "
                        + "AND %1$s.z = %2$s.z "
                        + "AND %1$s.y = %2$s.y "
                        + "AND %1$s.world = %2$s.world "
                        + "WHERE %1$s.id IS NULL",
                        shopTableName, temporaryShopTableName));
        setStockAndPurchasableQuantity = connection.prepareStatement
                (String.format("UPDATE %1$s INNER JOIN %2$s "
                        + "ON %1$s.x = %2$s.x "
                        + "AND %1$s.z = %2$s.z "
                        + "AND %1$s.y = %2$s.y "
                        + "AND %1$s.world = %2$s.world "
                        + "SET %1$s.stock = %2$s.stock,"
                        + "%1$s.purchasable_quantity = %2$s.purchasable_quantity",
                        shopTableName, temporaryShopTableName));
        insertShop = connection.prepareStatement
                (String.format("INSERT INTO %s VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?)",
                        shopTableName));
        insertShopReturnKey = connection.prepareStatement
                (String.format("INSERT INTO %s VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?)",
                        shopTableName),
                        Statement.RETURN_GENERATED_KEYS);
        selectIdFromShops = connection.prepareStatement
                (String.format("SELECT id FROM %s WHERE x=? AND z=? AND y=? AND world=?",
                        shopTableName));
        insertTransaction = connection.prepareStatement
                (String.format("INSERT INTO %s VALUES (NULL,NULL,?,?,?,?)",
                        transactionTableName));
        updateStock = connection.prepareStatement
                (String.format("UPDATE %s SET stock = stock + ?, "
                        + "purchasable_quantity = purchasable_quantity - ?"
                        + "WHERE id = ?",
                        shopTableName));
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
        if (deleteObsoleteShops != null) {
            try { deleteObsoleteShops.close(); } catch (SQLException e) {}
            deleteObsoleteShops = null;
        }
        if (insertIntoTemporary != null) {
            try { insertIntoTemporary.close(); } catch (SQLException e) {}
            insertIntoTemporary = null;
        }
        if (setStockAndPurchasableQuantity != null) {
            try { setStockAndPurchasableQuantity.close(); } catch (SQLException e) {}
            setStockAndPurchasableQuantity = null;
        }
        if (insertNewShops != null) {
            try { insertNewShops.close(); } catch (SQLException e) {}
            insertNewShops = null;
        }
        if (insertTransaction != null) {
            try { insertTransaction.close(); } catch (SQLException e) {}
            insertTransaction = null;
        }
        if (selectIdFromShops != null) {
            try { selectIdFromShops.close(); } catch (SQLException e) {}
            selectIdFromShops = null;
        }
        if (insertShop != null) {
            try { insertShop.close(); } catch (SQLException e) {}
            insertShop = null;
        }
        if (insertShopReturnKey != null) {
            try { insertShopReturnKey.close(); } catch (SQLException e) {}
            insertShopReturnKey = null;
        }
        if (updateStock != null) {
            try { updateStock.close(); } catch (SQLException e) {}
            updateStock = null;
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
            final List<ChestshopsRow> signs = new ArrayList<ChestshopsRow>();
            for (Sign sign : event.getSigns()) {
                if (ChestShopSign.isValid(sign)) {
                    signs.add(new ChestshopsRow(sign, sign.getLines()));
                }
            }

            if (! signs.isEmpty()) {
                database.runAsynchronously(new Runnable() {
                    public void run() {
                        syncSigns(chunkX, chunkZ, signs);
                    }
                });
            }
        } catch (Exception e) {
            logger.warning(e, "Failed to update scanned signs.");
        }
    }

    void syncSigns(int chunkX, int chunkZ, List<ChestshopsRow> scannedSigns) {
        Connection connection = database.getConnection();

        try {
            try {
                int batchSize = 0;
                for (ChestshopsRow row : scannedSigns) {
                    setInsertIntoTemporaryParams(row, insertIntoTemporary);
                    insertIntoTemporary.addBatch();
                    if (maxBatchSize <= ++batchSize) {
                        insertIntoTemporary.executeBatch();
                        batchSize = 0;
                    }
                }
                insertIntoTemporary.executeBatch();

                connection.setAutoCommit(false);

                deleteObsoleteShops.setInt(1, chunkX * Constants.chunkXSize);
                deleteObsoleteShops.setInt(2, (chunkX + 1) * Constants.chunkXSize);
                deleteObsoleteShops.setInt(3, chunkZ * Constants.chunkZSize);
                deleteObsoleteShops.setInt(4, (chunkZ + 1) * Constants.chunkZSize);
                deleteObsoleteShops.executeUpdate();
                setStockAndPurchasableQuantity.executeUpdate();
                insertNewShops.executeUpdate();

                connection.commit();

                truncateTemporary.executeUpdate();
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            logger.warning(e, "Failed to enter an entry into ChestShop shops table.");            
        }
    }
    
    void setInsertIntoTemporaryParams(ChestshopsRow row, PreparedStatement stmt) throws SQLException {
        stmt.clearParameters();
        stmt.setInt(1, row.x);
        stmt.setInt(2, row.z);
        stmt.setInt(3, row.y);
        stmt.setInt(4, worldTable.getId(row.world));
        if (row.owner != null) {
            stmt.setInt(5, playerTable.getId(row.owner));
        }
        stmt.setInt(6, row.quantity);
        stmt.setString(7, row.material);
        if (0.0 <= row.buyPrice) {
            stmt.setDouble(8, row.buyPrice);
        }
        if (0.0 <= row.sellPrice) {
            stmt.setDouble(9, row.sellPrice);
        }
        if (0 <= row.stock) {
            stmt.setInt(10, row.stock);
        }
        if (0 <= row.purchasableQuantity) {
            stmt.setInt(11, row.purchasableQuantity);
        }
    }

    @EventHandler
    void onShopCreatedEvent(ShopCreatedEvent event) {
        try {
            final ChestshopsRow row = new ChestshopsRow
                    (event.getSign(), event.getSignLines());
            database.runAsynchronously(new Runnable() {
                public void run() {
                    try {
                        setInsertIntoTemporaryParams(row, insertShop);
                        insertShop.executeUpdate();
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
            final ChestshopsRow row = new ChestshopsRow
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

    void deleteFromShops(ChestshopsRow row) throws SQLException {
        try (Statement statement = database.getConnection().createStatement()) {
            String stmt = String.format("DELETE FROM %s WHERE "
                    + "x = %d "
                    + "AND z = %d "
                    + "AND y = %d "
                    + "AND world = %d",
                    shopTableName, row.x, row.z, row.y, worldTable.getId(row.world));
            statement.executeUpdate(stmt);
        }
    }

    @EventHandler
    void onTransactionEvent(TransactionEvent event) {
        try {
            final ChestshopsRow row =
                    new ChestshopsRow(event.getSign(), event.getSign().getLines());
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

    void insertTransactionRecord(ChestshopsRow row, int playerId,
            TransactionType type, int amount) throws SQLException {
        Connection connection = database.getConnection();
        try (Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);

            selectIdFromShops.setInt(1, row.x);
            selectIdFromShops.setInt(2, row.z);
            selectIdFromShops.setInt(3, row.y);
            selectIdFromShops.setInt(4, worldTable.getId(row.world));

            int shopId;
            try (ResultSet resultSet = selectIdFromShops.executeQuery()) {            
                if (resultSet.next()) {
                    shopId = resultSet.getInt(1);
                } else {
                    setInsertIntoTemporaryParams(row, insertShopReturnKey);
                    insertShopReturnKey.executeUpdate();
                    try (ResultSet rs = insertShopReturnKey.getGeneratedKeys()) {
                        rs.next();
                        shopId = rs.getInt(1);
                    }            
                }
            }

            insertTransaction.setInt(1, shopId);
            insertTransaction.setInt(2, playerId);
            insertTransaction.setString(3, type.toString().toLowerCase());
            insertTransaction.setInt(4, amount);
            insertTransaction.executeUpdate();

            switch (type) {
            case BUY:
                updateStock.setInt(1, -amount);
                updateStock.setInt(2, amount);
                break;                
            case SELL:
                updateStock.setInt(1, amount);
                updateStock.setInt(2, -amount);
                break;
            default:
                assert false : "type = " + type;
            }
            updateStock.setInt(3, shopId);
            updateStock.executeUpdate();

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

        List<Sign> signs = new ArrayList<Sign>();
        
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

        for (Sign sign : signs) {            
            try {
                final ChestshopsRow row = new ChestshopsRow(sign, sign.getLines());
                database.runAsynchronously(new Runnable() {
                    public void run() {
                        try {
                            setInsertIntoTemporaryParams(row, insertShop);
                            insertShop.executeUpdate();
                        } catch (SQLException e) {
                            logger.warning(e, "Failed to enter a shop record.");
                        }
                    }
                });
            } catch (Exception e) {
                logger.warning(e, "Failed to enter shop record.");
            }
        }
    }

    void addAttachingChestShopSigns(Chest chest, List<Sign> target) {
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

    void enterShop(ChestshopsRow row) {
        Connection connection = database.getConnection();

        PreparedStatement insert = connection.prepareStatement
                (String.format("INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        temporaryShopTableName));
        
        try (PreparedStatement statement = connection.prepareStatement
                (String.format("DELETE FROM %s WHERE "
                        + "x = ? AND z = ? AND y = ? AND world = ?"
                        + "AND ",
                        shopTableName))) {
            try {
                statement.clearParameters();
                statement.setInt(1, row.x);
                statement.setInt(2, row.z);
                statement.setInt(3, row.y);
                statement.setInt(4, worldTable.getId(row.world));
                if (row.owner != null) {
                    statement.setInt(5, playerTable.getId(row.owner));
                }
                statement.setInt(6, row.quantity);
                statement.setString(7, row.material);
                if (0.0 <= row.buyPrice) {
                    statement.setDouble(8, row.buyPrice);
                }
                if (0.0 <= row.sellPrice) {
                    statement.setDouble(9, row.sellPrice);
                }
                if (0 <= row.stock) {
                    statement.setInt(10, row.stock);
                }
                if (0 <= row.purchasableQuantity) {
                    statement.setInt(11, row.purchasableQuantity);
                }
                statement.execute();

                connection.setAutoCommit(false);

                connection.prepareStatement
                (String.format("DELETE FROM %1$s "
                        + "USING %1$s LEFT JOIN %2$s "
                        + "ON %1$s.x = %2$s.x "
                        + "AND %1$s.z = %2$s.z "
                        + "AND %1$s.y = %2$s.y "
                        + "AND %1$s.world = %2$s.world "
                        + "WHERE ? <= %1$s.x AND %1$s.x < ? "
                        + "AND ? <= %1$s.z AND %1$s.z < ? "
                        + "AND (%2$s.x IS NULL "
                        + "OR ! (%1$s.owner <=> %2$s.owner) "
                        + "OR %1$s.quantity != %2$s.quantity "
                        + "OR %1$s.material != %2$s.material "
                        + "OR ! (%1$s.buy_price <=> %2$s.buy_price) "
                        + "OR ! (%1$s.sell_price <=> %2$s.sell_price))",
                        shopTableName, temporaryShopTableName));
                deleteObsoleteShops.setInt(1, chunkX * Constants.chunkXSize);
                deleteObsoleteShops.setInt(2, (chunkX + 1) * Constants.chunkXSize);
                deleteObsoleteShops.setInt(3, chunkZ * Constants.chunkZSize);
                deleteObsoleteShops.setInt(4, (chunkZ + 1) * Constants.chunkZSize);
                deleteObsoleteShops.executeUpdate();
                setStockAndPurchasableQuantity.executeUpdate();
                insertNewShops.executeUpdate();

                connection.commit();

                truncateTemporary.executeUpdate();
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            logger.warning(e, "Failed to enter an entry into ChestShop shops table.");            
        }
    }
}
