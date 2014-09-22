package jp.dip.myuminecraft.takenomics.chestshop;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import jp.dip.myuminecraft.takenomics.Constants;
import jp.dip.myuminecraft.takenomics.Database;
import jp.dip.myuminecraft.takenomics.Logger;
import jp.dip.myuminecraft.takenomics.SignScanEvent;
import jp.dip.myuminecraft.takenomics.UnknownPlayerException;
import jp.dip.myuminecraft.takenomics.models.PlayerTable;
import jp.dip.myuminecraft.takenomics.models.WorldTable;

import org.bukkit.Chunk;
import org.bukkit.OfflinePlayer;
import org.bukkit.block.Sign;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.java.JavaPlugin;

import com.Acrobot.Breeze.Utils.PriceUtil;
import com.Acrobot.ChestShop.ChestShop;
import com.Acrobot.ChestShop.Events.ShopCreatedEvent;
import com.Acrobot.ChestShop.Events.ShopDestroyedEvent;
import com.Acrobot.ChestShop.Events.TransactionEvent;
import com.Acrobot.ChestShop.Events.TransactionEvent.TransactionType;
import com.Acrobot.ChestShop.Signs.ChestShopSign;
import com.Acrobot.ChestShop.Utils.uName;

public class ChestShopMonitor implements Listener {

    class ChestshopsRow {
        int x;
        int z;
        int y;
        int world;
        String owner;
        int quantity;
        String material;
        double buyPrice;
        double sellPrice;
        
        ChestshopsRow(OfflinePlayer player, Sign sign, String[] lines)
                throws UnknownPlayerException, SQLException {
            x = sign.getX();
            z = sign.getZ();
            y = sign.getY();
            world = worldTable.getId(sign.getWorld());

            String nameOnSign = lines[ChestShopSign.NAME_LINE];
            String ownerName = nameOnSign.isEmpty()
                    ? player.getName()
                    : uName.getName(nameOnSign);

            OfflinePlayer ownerPlayer;
            if (ChestShopSign.isAdminShop(ownerName)) {
                ownerPlayer = null;
            } else {
                ownerPlayer = plugin.getServer().getOfflinePlayer(ownerName);
                if (ownerPlayer == null) {
                    throw new UnknownPlayerException
                    (String.format("ChestShop at %s:%d,%d,%d"
                            + " is owned by an unknown user '%s'.",
                            sign.getWorld().getName(), sign.getX(),
                            sign.getY(), sign.getZ(), ownerName));
                }
            }
            owner = ownerPlayer == null ? null : ownerPlayer.getName();
            
            quantity = Integer.parseInt(lines[ChestShopSign.QUANTITY_LINE]);
            material = lines[ChestShopSign.ITEM_LINE];

            String prices = lines[ChestShopSign.PRICE_LINE];
            buyPrice = PriceUtil.getBuyPrice(prices);
            sellPrice = PriceUtil.getSellPrice(prices);
        }
    }
    
    private JavaPlugin        plugin;
    private Logger            logger;
    private Database          database;
    private PlayerTable       playerTable;
    private WorldTable        worldTable;
    private PreparedStatement truncateTemporary;
    private PreparedStatement deleteObsoleteShops;
    private PreparedStatement insertNewShops;
    private PreparedStatement insertTransaction;
    private PreparedStatement selectIdFromShops;
    private PreparedStatement insertShop;
    private PreparedStatement insertShopReturnKey;

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
            logger.info("ChestShop is not found.");
            return false;
        }

        try {
            createTables();
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
    
    void createTables() throws SQLException {
        Connection connection = database.getConnection();

        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE IF NOT EXISTS chestshop_shops ("
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
                + "PRIMARY KEY (id),"
                + "UNIQUE (x,z,y,world),"
                + "INDEX (owner),"
                + "FOREIGN KEY (world) REFERENCES worlds (id) ON DELETE CASCADE,"
                + "FOREIGN KEY (owner) REFERENCES players (id) ON DELETE CASCADE"
                + ")");
        statement.execute("CREATE TABLE IF NOT EXISTS chestshop_transactions ("
                + "id INT UNSIGNED AUTO_INCREMENT NOT NULL,"
                + "timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,"
                + "shop SMALLINT UNSIGNED NOT NULL,"
                + "player MEDIUMINT UNSIGNED NOT NULL,"
                + "type ENUM('buy', 'sell') NOT NULL,"
                + "quantity SMALLINT NOT NULL,"
                + "PRIMARY KEY (id),"
                + "FOREIGN KEY (shop) REFERENCES chestshop_shops (id) ON DELETE CASCADE,"
                + "FOREIGN KEY (player) REFERENCES players (id) ON DELETE CASCADE"
                + ")");
        statement.execute("CREATE TEMPORARY TABLE IF NOT EXISTS chestshop_scanned_shops ("
                + "x SMALLINT NOT NULL,"
                + "z SMALLINT NOT NULL,"
                + "y SMALLINT NOT NULL,"
                + "world SMALLINT UNSIGNED NOT NULL,"
                + "owner INT UNSIGNED,"
                + "quantity SMALLINT UNSIGNED NOT NULL,"
                + "material VARCHAR(16) CHARACTER SET ascii NOT NULL,"
                + "buy_price DOUBLE,"
                + "sell_price DOUBLE,"
                + "UNIQUE (x,z,y,world)"
                + ") ENGINE=MEMORY");
    }
    
    void prepareStatements() throws SQLException {
        Connection connection = database.getConnection();

        truncateTemporary = connection.prepareStatement("TRUNCATE chestshop_scanned_shops");
        deleteObsoleteShops = connection.prepareStatement("DELETE FROM chestshop_shops "
                + "USING chestshop_shops LEFT JOIN chestshop_scanned_shops "
                + "ON chestshop_shops.x = chestshop_scanned_shops.x "
                + "AND chestshop_shops.z = chestshop_scanned_shops.z "
                + "AND chestshop_shops.y = chestshop_scanned_shops.y "
                + "AND chestshop_shops.world = chestshop_scanned_shops.world "
                + "WHERE ? <= chestshop_shops.x AND chestshop_shops.x < ? "
                + "AND ? <= chestshop_shops.z AND chestshop_shops.z < ? "
                + "AND (chestshop_scanned_shops.x IS NULL "
                + "OR ! chestshop_shops.owner <=> chestshop_scanned_shops.owner "
                + "OR chestshop_shops.quantity != chestshop_scanned_shops.quantity "
                + "OR chestshop_shops.material != chestshop_scanned_shops.material "
                + "OR chestshop_shops.buy_price != chestshop_scanned_shops.buy_price "
                + "OR chestshop_shops.sell_price != chestshop_scanned_shops.sell_price)");
        insertNewShops = connection.prepareStatement("INSERT INTO chestshop_shops "
                + "SELECT NULL,"
                + "chestshop_scanned_shops.x,"
                + "chestshop_scanned_shops.z,"
                + "chestshop_scanned_shops.y,"
                + "chestshop_scanned_shops.world,"
                + "chestshop_scanned_shops.owner,"
                + "chestshop_scanned_shops.quantity,"
                + "chestshop_scanned_shops.material,"
                + "chestshop_scanned_shops.buy_price,"
                + "chestshop_scanned_shops.sell_price "
                + "FROM chestshop_scanned_shops LEFT JOIN chestshop_shops "
                + "ON chestshop_shops.x = chestshop_scanned_shops.x "
                + "AND chestshop_shops.z = chestshop_scanned_shops.z "
                + "AND chestshop_shops.y = chestshop_scanned_shops.y "
                + "AND chestshop_shops.world = chestshop_scanned_shops.world "
                + "WHERE chestshop_shops.id IS NULL");
        insertShop = connection.prepareStatement
                ("INSERT INTO chestshop_shops VALUES (NULL,?,?,?,?,?,?,?,?,?)");
        insertShopReturnKey = connection.prepareStatement
                ("INSERT INTO chestshop_shops VALUES (NULL,?,?,?,?,?,?,?,?,?)",
                  Statement.RETURN_GENERATED_KEYS);
        selectIdFromShops = connection.prepareStatement
                ("SELECT id FROM chestshop_shops WHERE x=? AND z=? AND y=? AND world=?");
        insertTransaction = connection.prepareStatement
                ("INSERT INTO chestshop_transactions VALUES (NULL,NULL,?,?,?,?)");
    }

    public void disable() {
        try {
            if (truncateTemporary != null) {
                truncateTemporary.close();
                truncateTemporary = null;
            }
            if (deleteObsoleteShops != null) {
                deleteObsoleteShops.close();
                deleteObsoleteShops = null;
            }
            if (insertNewShops != null) {
                insertNewShops.close();
                insertNewShops = null;
            }
            if (insertShop != null) {
                insertShop.close();
                insertShop = null;
            }
            if (insertShopReturnKey != null) {
                insertShopReturnKey.close();
                insertShopReturnKey = null;
            }
            if (selectIdFromShops != null) {
                selectIdFromShops.close();
                selectIdFromShops = null;
            }
            if (insertTransaction != null) {
                insertTransaction.close();
                insertTransaction = null;
            }
        } catch (SQLException e1) {
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
                    signs.add(new ChestshopsRow(null, sign, sign.getLines()));
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

        try (Statement statement = connection.createStatement()) {
            StringBuilder insert =
                    new StringBuilder("INSERT INTO chestshop_scanned_shops VALUES ");
            boolean first = true;
            for (ChestshopsRow row : scannedSigns) {
                if (! first) {
                    insert.append(",");
                } else {
                    first = false;
                }
                
                insert.append(String.format("(%d,%d,%d,%d",
                        row.x, row.z, row.y, row.world));
                insert.append(row.owner != null
                        ? String.format(",%d", playerTable.getId(row.owner))
                        : ",NULL");
                insert.append(String.format(",%d,'%s'",
                        row.quantity, Database.escapeSingleQuotes(row.material)));
                appendPrice(insert, row.buyPrice);
                appendPrice(insert, row.sellPrice);
                insert.append(")");
            }

            connection.setAutoCommit(false);

            statement.executeUpdate(insert.toString());
            deleteObsoleteShops.setInt(1, chunkX * Constants.chunkXSize);
            deleteObsoleteShops.setInt(2, (chunkX + 1) * Constants.chunkXSize);
            deleteObsoleteShops.setInt(3, chunkZ * Constants.chunkZSize);
            deleteObsoleteShops.setInt(4, (chunkZ + 1) * Constants.chunkZSize);
            deleteObsoleteShops.executeUpdate();
            insertNewShops.executeUpdate();
            truncateTemporary.executeUpdate();

            connection.commit();
        } catch (SQLException e) {
            logger.warning(e, "Failed to sync chestshop_shops table.");
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                logger.warning(e, "Failed to disable autoCommit.");
            }
        }
    }
    
    void appendPrice(StringBuilder target, double price) {
        if (price < 0.0) {
            target.append(",NULL");
        } else {
            target.append(String.format(",%f", price));
        }
    }            

    @EventHandler
    void onShopCreatedEvent(ShopCreatedEvent event) {
        try {
            final ChestshopsRow row = new ChestshopsRow
                    (event.getPlayer(), event.getSign(), event.getSignLines());
            database.runAsynchronously(new Runnable() {
                public void run() {
                    try {
                        insertIntoShops(row);
                    } catch (SQLException e) {
                        logger.warning(e, "Failed to enter shop record.");
                    }
                }
            });
        } catch (Exception e) {
            logger.warning(e, "Failed to enter shop record.");
        }
    }

    void setInsertIntoShopsParams(ChestshopsRow row, PreparedStatement stmt) throws SQLException {
        stmt.setInt(1, row.x);
        stmt.setInt(2, row.z);
        stmt.setInt(3, row.y);
        stmt.setInt(4, row.world);
        stmt.setInt(5, playerTable.getId(row.owner));
        stmt.setInt(6, row.quantity);
        stmt.setString(7, row.material);
        if (row.buyPrice != -1.0) {
            stmt.setDouble(8, row.buyPrice);
        } else {
            stmt.setNull(8, Types.DOUBLE);
        }
        if (row.sellPrice != -1.0) {
            stmt.setDouble(9, row.sellPrice);
        } else {
            stmt.setNull(9, Types.DOUBLE);
        }
    }

    void insertIntoShops(ChestshopsRow row) throws SQLException {
        setInsertIntoShopsParams(row, insertShop);
        insertShop.executeUpdate();
    }

    int insertIntoShopsReturnKey(ChestshopsRow row) throws SQLException {
        setInsertIntoShopsParams(row, insertShopReturnKey);
        insertShopReturnKey.executeUpdate();
        try (ResultSet rs = insertShopReturnKey.getGeneratedKeys()) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @EventHandler
    void onShopDestroyedEvent(ShopDestroyedEvent event) {
        try {
            final ChestshopsRow row = new ChestshopsRow
                    (null, event.getSign(), event.getSign().getLines());
            database.runAsynchronously(new Runnable() {
                public void run() {
                    try {
                        deleteFromShops(row);
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
            String stmt = String.format("DELETE FROM chestshop_shops WHERE "
                    + "x = %d "
                    + "AND z = %d "
                    + "AND y = %d "
                    + "AND world = %d",
                    row.x, row.z, row.y, row.world);
            statement.executeUpdate(stmt);
        }
    }

    @EventHandler
    void onTransactionEvent(TransactionEvent event) {
        try {
            final ChestshopsRow row =
                    new ChestshopsRow(null, event.getSign(), event.getSign().getLines());
            final int amount = event.getStock()[0].getAmount();
            final TransactionType type = event.getTransactionType();
            final int playerId = playerTable.getId(event.getClient());
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
            selectIdFromShops.setInt(4, row.world);
            
            int shopId;
            try (ResultSet resultSet = selectIdFromShops.executeQuery()) {            
                shopId = resultSet.next()
                        ? resultSet.getInt(1)
                        : insertIntoShopsReturnKey(row);
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
  
}
