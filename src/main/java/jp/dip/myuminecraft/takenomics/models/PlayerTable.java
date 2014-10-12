package jp.dip.myuminecraft.takenomics.models;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import jp.dip.myuminecraft.takenomics.Constants;
import jp.dip.myuminecraft.takenomics.Database;
import jp.dip.myuminecraft.takenomics.Logger;
import jp.dip.myuminecraft.takenomics.UnknownPlayerException;
import net.milkbowl.vault.economy.Economy;

import org.bukkit.OfflinePlayer;
import org.bukkit.plugin.java.JavaPlugin;

public class PlayerTable {

    static final int   currentSchemaVersion = 2;
    static final int   maxBatchSize         = 1000;
    JavaPlugin         plugin;
    Logger             logger;
    Database           database;
    Economy            economy;
    String             tableName;
    Map<UUID, Integer> idCache;
    Map<String, UUID>  nameCache;
    PreparedStatement  insertEntry;
    PreparedStatement  findEntry;
    PreparedStatement  updateName;
    PreparedStatement  eraseStaleName;
    PreparedStatement  findFromName;
    PreparedStatement  updateBalance;

    public PlayerTable(JavaPlugin plugin,
            Logger logger, Database database, Economy economy) {
        this.plugin = plugin;
        this.logger = logger;
        this.database = database;
        this.economy = economy;
        this.idCache = Collections.synchronizedMap
                (new HashMap<UUID, Integer>());
        this.nameCache = new HashMap<String, UUID>();
    }
    
    public String getTableName() {
        return tableName;
    }

    public boolean enable() {
        if (database == null || economy == null) {
            return true;
        }

        Connection connection = database.getConnection();            

        tableName = database.getTablePrefix() + "players";
        boolean needToSync = false;
        try {
            if (! database.hasTable(tableName)) {
                createTable(tableName);
                needToSync = true;
            } else {
                int version = database.getVersion(tableName);
                switch (version) {
                case currentSchemaVersion:
                    break;

                case 1:
                    upgradeFromV1toV2();
                    needToSync = true;
                    break;

                default:
                   logger.warning("Unknown table schema version:"
                           + " table %s, version %d",
                           tableName, version);
                   disable();
                   return true;
                }
            }

            findEntry = connection.prepareStatement
                    (String.format("SELECT id, name FROM %s WHERE uuid=?", tableName));
            insertEntry = connection.prepareStatement
                    (String.format("INSERT INTO %s VALUES (NULL,?,?,?)", tableName),
                            Statement.RETURN_GENERATED_KEYS);
            updateName = connection.prepareStatement
                    (String.format("UPDATE %s SET name=? WHERE id=?", tableName));
            eraseStaleName = connection.prepareStatement
                    (String.format("UPDATE %s SET name=NULL WHERE uuid != ? AND name = ?",
                            tableName));
            findFromName = connection.prepareStatement
                    (String.format("SELECT id, uuid FROM %s WHERE name=?", tableName));
            updateBalance = connection.prepareStatement
                    (String.format("UPDATE %s SET balance=? WHERE id=?", tableName));

            if (needToSync) {
                sync();
            }

            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery
                        (String.format("SELECT id, uuid, name FROM %s", tableName));
                while (rs.next()) {
                    UUID uuid = Database.toUUID(rs.getBytes(2));
                    String name = rs.getString(3);
                    idCache.put(uuid,  rs.getInt(1));
                    if (name != null) {
                        nameCache.put(name.toLowerCase(), uuid);
                    }
                }
            }
        } catch (SQLException e) {
            logger.warning(e, "Failed to initialize %s.", tableName);
            disable();
            return true;
        }

        return false;
    }

    public void sync() throws SQLException {
        Connection connection = database.getConnection();
        try (Statement stmt = connection.createStatement()) {

            String tempTableName = tableName + "_temp";
            stmt.executeUpdate(String.format(
                    "CREATE TEMPORARY TABLE IF NOT EXISTS %s LIKE %s", tempTableName, tableName));

            try (PreparedStatement insert =
                    connection.prepareStatement
                    (String.format("INSERT INTO %s VALUES (NULL,?,?,?)", tempTableName))) {
                byte[] uuid = new byte[Constants.sizeOfUUID];
                int batchCount = 0;
                for (OfflinePlayer player : plugin.getServer().getOfflinePlayers()) {
                    Database.toBytes(player.getUniqueId(), uuid);
                    insert.setBytes(1, uuid);
                    insert.setString(2, player.getName());
                    insert.setDouble(3, economy.getBalance(player));
                    insert.addBatch();
                    logger.info("import %s %s", player.getName(), player.getUniqueId().toString());
                    if (maxBatchSize < ++batchCount) {
                        insert.executeBatch();
                        batchCount = 0;
                    }
                }
                insert.executeBatch();
            }

            // Remove any player entries that doesn't exist in the offline players list.
            stmt.executeUpdate(String.format(
                    "DELETE t1 FROM %s t1 LEFT JOIN %s t2 "
                    + "USING (uuid) WHERE t2.id IS NULL",
                    tableName, tempTableName));
            
            // Nullify names given to other online players.
            stmt.executeUpdate(String.format(
                    "UPDATE %s t1 INNER JOIN %s t2 USING (name) "
                    + "SET t1.name = NULL WHERE t1.uuid != t2.uuid",
                    tableName, tempTableName));

            // Replace existing entries by the info in the offline players list,
            // and insert non-existing entries.
            stmt.executeUpdate(String.format(
                    "REPLACE %s SELECT t1.id, t2.uuid, t2.name, t2.balance "
                    + "FROM %s t1 RIGHT JOIN %s t2 using (uuid)",
                    tableName, tableName, tempTableName));

            stmt.executeUpdate(String.format(
                    "TRUNCATE %s", tempTableName));
        }
    }

    void createTable(String newTableName) throws SQLException {
        try (Statement stmt = database.getConnection().createStatement()) {
            stmt.execute(String.format
                    ("CREATE TABLE %s ("
                            + "id MEDIUMINT UNSIGNED AUTO_INCREMENT NOT NULL,"
                            + "uuid BINARY(16) NOT NULL,"
                            + "name VARCHAR(16) CHARACTER SET ascii,"
                            + "balance DOUBLE,"
                            + "PRIMARY KEY (id),"
                            + "UNIQUE (uuid),"
                            + "UNIQUE (name))",
                            newTableName));
        }
        database.setVersion(tableName, currentSchemaVersion);
    }

    void upgradeFromV1toV2() throws SQLException {
        Connection connection = database.getConnection();
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(String.format(
                    "ALTER TABLE %s ADD COLUMN balance DOUBLE",
                    tableName));
        }
        database.setVersion(tableName, 2);
    }

    public void disable() {
        idCache.clear();
        nameCache.clear();
        if (findEntry != null) {
            try { findEntry.close(); } catch (SQLException e) {}
            findEntry = null;
        }
        if (updateName != null) {
            try { updateName.close(); } catch (SQLException e) {}
            updateName = null;
        }
        if (eraseStaleName != null) {
            try { eraseStaleName.close(); } catch (SQLException e) {}
            eraseStaleName = null;
        }
        if (findFromName != null) {
            try { findFromName.close(); } catch (SQLException e) {}
            findFromName = null;
        }
        if (updateBalance != null) {
            try { updateBalance.close(); } catch (SQLException e) {}
            updateBalance = null;
        }
    }

    public int getId(UUID uuid) throws SQLException, UnknownPlayerException {
        Integer id = idCache.get(uuid);
        if (id != null) {
            return id;
        }

        OfflinePlayer player = plugin.getServer().getOfflinePlayer(uuid);
        String name = player.getName();
        if (player == null || name == null) {
            throw new UnknownPlayerException("unknown UUID: " + uuid.toString());
        }

        return enter(uuid, name);
    }

    public UUID getUniqueIdForName(String name) {
        return nameCache.get(name.toLowerCase());
    }

    public int enter(UUID uuid, String name) throws SQLException {
        int id = -1;
        Connection connection = database.getConnection();

        try {
            connection.setAutoCommit(false);

            byte[] uuidBytes = new byte[Constants.sizeOfUUID];     
            Database.toBytes(uuid, uuidBytes);

            eraseStaleName.setBytes(1, uuidBytes);
            eraseStaleName.setString(2, name);
            eraseStaleName.executeUpdate();

            findEntry.setBytes(1, uuidBytes);
            try (ResultSet result = findEntry.executeQuery()) {
                if (result.next()) {
                    // Update column `name` of table `players` if necessary
                    id = result.getInt(1);
                    String nameInDB = result.getString(2);
                    if (nameInDB == null || ! nameInDB.equals(name)) {
                        updateName.setString(1, name);
                        updateName.setInt(2, id);
                        updateName.execute();
                    }
                } else {
                    // Insert new entry to table `players`.
                    insertEntry.setBytes(1, uuidBytes);
                    insertEntry.setString(2, name);
                    insertEntry.setDouble(3, 0.0);
                    insertEntry.executeUpdate();
                    try (ResultSet updateResult = insertEntry.getGeneratedKeys()) {
                        updateResult.next();
                        id = updateResult.getInt(1);
                    }
                }
            }

            connection.commit();
        } finally {
            connection.setAutoCommit(true);
        }

        idCache.put(uuid, id);
        nameCache.put(name.toLowerCase(), uuid);

        return id;
    }

    public void updateBalance(UUID player, double balance) throws SQLException, UnknownPlayerException {
        int id = getId(player);
        updateBalance.setDouble(1, balance);
        updateBalance.setInt(2, id);
        updateBalance.executeUpdate();
    }

}
