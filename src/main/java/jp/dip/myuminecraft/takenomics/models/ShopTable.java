package jp.dip.myuminecraft.takenomics.models;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import jp.dip.myuminecraft.takecore.Logger;
import jp.dip.myuminecraft.takenomics.Constants;
import jp.dip.myuminecraft.takenomics.Database;
import jp.dip.myuminecraft.takenomics.ShopValidator;
import jp.dip.myuminecraft.takenomics.UnknownPlayerException;

import org.bukkit.Server;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.block.BlockState;
import org.bukkit.plugin.java.JavaPlugin;

public class ShopTable {

    public static class Shop {
        public int    x;
        public int    z;
        public int    y;
        public String world;
        public UUID   owner;
        public int    quantity;
        public String material;
        public double buyPrice;
        public double sellPrice;
        public int    stock;
        public int    purchasableQuantity;

        public Shop(int x, int z, int y, String world, UUID owner, int quantity,
                String material, double buyPrice, double sellPrice,
                int stock, int purchasableQuantity) {
            this.x = x;
            this.z = z;
            this.y = y;
            this.world = world;
            this.owner = owner;
            this.quantity = quantity;
            this.material = material;
            this.buyPrice = buyPrice;
            this.sellPrice = sellPrice;
            this.stock = stock;
            this.purchasableQuantity = purchasableQuantity;
        }
    }

    public static final int currentSchemaVersion = 1;
    public static final int maxBatchSize         = 100;
    JavaPlugin              plugin;
    Logger                  logger;
    Database                database;
    PlayerTable             playerTable;
    WorldTable              worldTable;
    String                  tableName;
    String                  temporaryTableName;
    Set<ShopValidator>      validators = new HashSet<ShopValidator>();
    PreparedStatement       truncateTemporary;
    PreparedStatement       insertIntoTemporary;
    PreparedStatement       deleteObsoleteShopsInXZRange;
    PreparedStatement       deleteObsoleteShops;
    PreparedStatement       replaceShops;
    PreparedStatement       getIdStatement;
    PreparedStatement       deleteShop;
    PreparedStatement       scrubShops;

    public ShopTable(JavaPlugin plugin, Logger logger, Database database,
            PlayerTable playerTable, WorldTable worldTable) {
        this.plugin = plugin;
        this.logger = logger;
        this.database = database;
        this.playerTable = playerTable;
        this.worldTable = worldTable;
    }
    
    public String getTableName() {
        return tableName;
    }

    public boolean enable() {
        if (database == null || playerTable == null || worldTable == null) {
            return true;
        }
        
        String tablePrefix = database.getTablePrefix();
        tableName = tablePrefix + "shops";
        temporaryTableName = tablePrefix + "shops_temp";

        try {
            if (! database.hasTable(tableName)) {
                createShopTable(tableName);
            } else {
                int existingTableVersion = database.getVersion(tableName);  
                switch (existingTableVersion) {  

                case currentSchemaVersion:
                    break;

                case 0:
                    upgradeShopTableFromVersion0();
                    break;

                default:
                    logger.warning("%s: Unsupported table schema version %d",
                            tableName, existingTableVersion);
                    disable();
                    return true;
                }
            }

            initializeTemporaryTable();
            prepareStatements();
            truncateTemporary.executeUpdate();
        } catch (SQLException e) {
            logger.warning(e, "Failed to initialize shop table.");
            disable();
            return true;
        }

        return false;
    }
    
    public void disable() {
        if (truncateTemporary != null) {
            try { truncateTemporary.close(); } catch (SQLException e) {}
            truncateTemporary = null;
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
        if (insertIntoTemporary != null) {
            try { insertIntoTemporary.close(); } catch (SQLException e) {}
            insertIntoTemporary = null;
        }
        if (replaceShops != null) {
            try { replaceShops.close(); } catch (SQLException e) {}
            replaceShops = null;
        }
        if (getIdStatement != null) {
            try { getIdStatement.close(); } catch (SQLException e) {}
            getIdStatement = null;
        }
        if (deleteShop != null) {
            try { deleteShop.close(); } catch (SQLException e) {}
            deleteShop = null;
        }
    }

    public void put(Shop shop) throws SQLException, UnknownPlayerException {
        Connection connection = database.getConnection();
        connection.setAutoCommit(false);
        try {
            setInsertShopParams(shop, insertIntoTemporary);
            insertIntoTemporary.executeUpdate();
            deleteObsoleteShops.executeUpdate();
            replaceShops.executeUpdate();
            truncateTemporary.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    public void put(Collection<Shop> shops) throws SQLException, UnknownPlayerException {
        Connection connection = database.getConnection();
        connection.setAutoCommit(false);
        try {
            int batchSize = 0;
            for (Shop shop : shops) {
                setInsertShopParams(shop, insertIntoTemporary);
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
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    public void remove(String world, int x, int y, int z) throws SQLException {
        deleteShop.setInt(1, x);
        deleteShop.setInt(2, z);
        deleteShop.setInt(3, y);
        deleteShop.setInt(4, worldTable.getId(world));
        deleteShop.executeUpdate();
    }
    
    public int getId(String world, int x, int y, int z) throws SQLException {
        getIdStatement.setInt(1, x);
        getIdStatement.setInt(2, z);
        getIdStatement.setInt(3, y);
        getIdStatement.setInt(4, worldTable.getId(world));

        try (ResultSet resultSet = getIdStatement.executeQuery()) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }

    public void addValidator(ShopValidator validator) {
        validators.add(validator);
    }

    public void removeValidator(ShopValidator validator) {
        validators.remove(validator);
    }

    public void runScrubTransaction() throws SQLException, UnknownPlayerException {
        Server server = plugin.getServer();
        Connection connection = database.getConnection();
        connection.setAutoCommit(false);

        try (Statement stmt = connection.createStatement()) {

            int batchSize = 0;
            try (ResultSet rs = stmt
                    .executeQuery(String.format("SELECT x,z,y,t2.name "
                                    + "FROM %s t1 INNER JOIN %s t2 ON t1.world = t2.id",
                                    tableName, worldTable.getTableName()))) {
                while (rs.next()) {
                    int x = rs.getInt(1);
                    int z = rs.getInt(2);
                    int y = rs.getInt(3);
                    String worldName = rs.getString(4);
                    World world = server.getWorld(worldName);

                    int chunkX = x / Constants.chunkXSize;
                    int chunkZ = z / Constants.chunkZSize;
                    if (!world.isChunkLoaded(chunkX, chunkZ)) {
                        world.loadChunk(chunkX, chunkZ);
                    }

                    Block block = world.getBlockAt(x, y, z);

                    for (ShopValidator validator : validators) {
                        Shop shop = validator.validate(block.getState());
                        if (shop != null) {
                            setInsertShopParams(shop, insertIntoTemporary);
                            insertIntoTemporary.addBatch();
                            if (maxBatchSize <= ++batchSize) {
                                insertIntoTemporary.executeBatch();
                                batchSize = 0;
                            }
                            break;
                        }
                    }
                }
                insertIntoTemporary.executeBatch();

                scrubShops.executeUpdate();
                replaceShops.executeUpdate();
                truncateTemporary.executeUpdate();
            }

            connection.commit();
        } catch (Throwable t) {
            connection.rollback();
            throw t;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    public void runSyncChunkTransaction(
            int chunkX, int chunkZ, Collection<BlockState> states)
            throws SQLException {
        Connection connection = database.getConnection();
        connection.setAutoCommit(false);

        try {
            int batchSize = 0;
            for (BlockState state : states) {
                for (ShopValidator validator : validators) {
                    try {
                        Shop shop = validator.validate(state);
                        if (shop != null) {
                            setInsertShopParams(shop, insertIntoTemporary);
                            insertIntoTemporary.addBatch();
                            if (maxBatchSize <= ++batchSize) {
                                insertIntoTemporary.executeBatch();
                                batchSize = 0;
                            }
                            break;
                        }
                    } catch (Throwable t) {
                        logger.warning(t, "Exception from a shop validator.");
                    }
                }
            }
            insertIntoTemporary.executeBatch();

            deleteObsoleteShopsInXZRange.setInt(1, chunkX       * Constants.chunkXSize);
            deleteObsoleteShopsInXZRange.setInt(2, (chunkX + 1) * Constants.chunkXSize);
            deleteObsoleteShopsInXZRange.setInt(3, chunkZ       * Constants.chunkZSize);
            deleteObsoleteShopsInXZRange.setInt(4, (chunkZ + 1) * Constants.chunkZSize);
            deleteObsoleteShopsInXZRange.executeUpdate();
            replaceShops.executeUpdate();
            truncateTemporary.executeUpdate();

            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
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
                            tableName, worldTable.getTableName(), playerTable.getTableName()));
        }
        database.setVersion(tableName, currentSchemaVersion);
    }

    void upgradeShopTableFromVersion0() throws SQLException {
        Connection connection = database.getConnection();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format("ALTER TABLE %s ADD COLUMN stock SMALLINT UNSIGNED, "
                    + "ADD COLUMN purchasable_quantity SMALLINT UNSIGNED,"
                    + "DROP INDEX owner",
                    tableName));
        }
        database.setVersion(tableName, currentSchemaVersion);
    }

    void initializeTemporaryTable() throws SQLException {
        Connection connection = database.getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format("DROP TABLE IF EXISTS %s", temporaryTableName));
            statement.execute(String.format("CREATE TEMPORARY TABLE %s LIKE %s",
                            temporaryTableName, tableName));
        }
    }

    void prepareStatements() throws SQLException {
        Connection connection = database.getConnection();

        truncateTemporary = connection.prepareStatement
                (String.format("TRUNCATE %s", temporaryTableName));

        deleteObsoleteShopsInXZRange = connection.prepareStatement
                (String.format("DELETE t1 FROM %s t1 LEFT JOIN %s t2 USING (x,z,y,world) "
                        + "WHERE ? <= t1.x AND t1.x < ? "
                        + "AND ? <= t1.z AND t1.z < ? "
                        + "AND (! (t1.owner <=> t2.owner) "
                        + "OR t1.quantity != t2.quantity "
                        + "OR t1.material != t2.material "
                        + "OR ! (t1.buy_price <=> t2.buy_price) "
                        + "OR ! (t1.sell_price <=> t2.sell_price))",
                        tableName, temporaryTableName));

        deleteObsoleteShops = connection.prepareStatement
                (String.format("DELETE t1 FROM %s t1 INNER JOIN %s t2 USING (x,z,y,world) "
                        + "WHERE ! (t1.owner <=> t2.owner) "
                        + "OR t1.quantity != t2.quantity "
                        + "OR t1.material != t2.material "
                        + "OR ! (t1.buy_price <=> t2.buy_price) "
                        + "OR ! (t1.sell_price <=> t2.sell_price)",
                        tableName, temporaryTableName));

        scrubShops = connection.prepareStatement
                (String.format("DELETE t1 FROM %s t1 LEFT JOIN %s t2 USING (x,z,y,world) "
                        + "WHERE ! (t1.owner <=> t2.owner) "
                        + "OR t1.quantity != t2.quantity "
                        + "OR t1.material != t2.material "
                        + "OR ! (t1.buy_price <=> t2.buy_price) "
                        + "OR ! (t1.sell_price <=> t2.sell_price)",
                        tableName, temporaryTableName));

        insertIntoTemporary = connection.prepareStatement
                (String.format("INSERT INTO %s VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?)",
                        temporaryTableName));

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
                        tableName, tableName, temporaryTableName));

        getIdStatement = connection.prepareStatement
                (String.format("SELECT id FROM %s WHERE x=? AND z=? AND y=? AND world=?",
                        tableName));

        deleteShop = connection.prepareStatement
                (String.format("DELETE FROM %s WHERE "
                        + "x = ? "
                        + "AND z = ? "
                        + "AND y = ? "
                        + "AND world = ?",
                        tableName));
    }

    void setInsertShopParams(Shop row, PreparedStatement stmt) throws SQLException, UnknownPlayerException {
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

}
