package jp.dip.myuminecraft.takenomics.models;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import jp.dip.myuminecraft.takenomics.Constants;
import jp.dip.myuminecraft.takenomics.Database;
import jp.dip.myuminecraft.takenomics.Logger;

import org.bukkit.OfflinePlayer;
import org.bukkit.Server;
import org.bukkit.plugin.java.JavaPlugin;

public class PlayerTable {

    static final int   currentSchemaVersion = 1;
    static final int   maxBatchSize         = 1000;
    JavaPlugin         plugin;
    Logger             logger;
    Database           database;
    String             tableName;
    Map<UUID, Integer> cache                = new HashMap<UUID, Integer>();
    PreparedStatement  insertEntry;
    PreparedStatement  findEntry;
    PreparedStatement  updateName;
    PreparedStatement  eraseStaleName;

    public PlayerTable(JavaPlugin plugin, Logger logger, Database database) {
        this.plugin = plugin;
        this.logger = logger;
        this.database = database;
    }
    
    public String getTableName() {
        return tableName;
    }

    public boolean enable() {
        if (database == null) {
            return false;
        }

        Connection connection = database.getConnection();            

        tableName = database.getTablePrefix() + "players";

        try {
            if (! database.hasTable(tableName)) {
                createEmptyTable(tableName);
                database.setVersion(tableName, currentSchemaVersion);
                importFromOfflinePlayerList();
            } else {
                int version = database.getVersion(tableName);
                if (version < currentSchemaVersion) {
                    if (! upgradeTable(version)) {
                        disable();
                        return false;
                    }
                } else if (currentSchemaVersion < version) {
                    logger.warning("The schema version of %s is higher (%d) "
                            + "than the current version (%d).",
                            tableName, version, currentSchemaVersion);
                    disable();
                    return false;
                }
            }

            findEntry = connection.prepareStatement
                    (String.format("SELECT id, name FROM %s WHERE uuid=?", tableName));
            insertEntry = connection.prepareStatement
                    (String.format("INSERT INTO %s VALUES (NULL,?,?)", tableName),
                            Statement.RETURN_GENERATED_KEYS);
            updateName = connection.prepareStatement
                    (String.format("UPDATE %s SET name=? WHERE id=?", tableName));
            eraseStaleName = connection.prepareStatement
                    (String.format("UPDATE %s SET name=NULL WHERE uuid != ? AND name = ?",
                            tableName));
        } catch (SQLException e) {
            logger.warning(e, "Failed to initialize %s.", tableName);
            disable();
            return false;
        }
        
        return true;
    }

    void importFromOfflinePlayerList() throws SQLException {
        Connection connection = database.getConnection();
        try (PreparedStatement insert =
                connection.prepareStatement
                (String.format("INSERT INTO %s VALUES (NULL,?,?)", tableName))) {
            byte[] uuid = new byte[Constants.sizeOfUUID];
            int batchCount = 0;
            for (OfflinePlayer player : plugin.getServer().getOfflinePlayers()) {
                Database.toBytes(player.getUniqueId(), uuid);
                insert.setBytes(1, uuid);
                insert.setString(2, player.getName());
                insert.addBatch();
                if (maxBatchSize < ++batchCount) {
                    insert.executeBatch();
                    batchCount = 0;
                }
            }
            insert.executeBatch();
        }
    }

    void createEmptyTable(String newTableName) throws SQLException {
        try (Statement stmt = database.getConnection().createStatement()) {
            stmt.execute(String.format
                    ("CREATE TABLE %s ("
                            + "id MEDIUMINT UNSIGNED AUTO_INCREMENT NOT NULL,"
                            + "uuid BINARY(16) NOT NULL,"
                            + "name VARCHAR(16) CHARACTER SET ascii,"
                            + "PRIMARY KEY (id),"
                            + "UNIQUE (uuid),"
                            + "UNIQUE (name))",
                            newTableName));
        }
    }

    private boolean upgradeTable(int dbVersion) throws SQLException {
        switch (dbVersion) {
        
        case 0:
            upgradeFromVersion0();
            break;
            
        default:
           logger.warning("Unknown table schema version: table %s, version %d",
                   tableName, dbVersion);
           return false;
        }
        
        return true;
    }
    
    @SuppressWarnings("deprecation")
    void upgradeFromVersion0() throws SQLException {
        String newTableName = tableName + "_new";
        createEmptyTable(newTableName);

        Server server = plugin.getServer();
        Connection connection = database.getConnection();

        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery
                    (String.format("SELECT id, name FROM %s", tableName))) {
                try (PreparedStatement insert = connection.prepareStatement
                        (String.format("INSERT INTO %s VALUES (?,?,?)", newTableName))) {
                    byte[] uuid = new byte[Constants.sizeOfUUID];
                    int batchSize = 0;
                    while (rs.next()) {
                        String name = rs.getString(2);
                        Database.toBytes(server.getOfflinePlayer(name).getUniqueId(), uuid);
                        insert.setInt(1, rs.getInt(1));
                        insert.setBytes(2, uuid);
                        insert.setString(3, name);
                        insert.addBatch();
                        if (maxBatchSize <= ++batchSize) {
                            insert.executeBatch();
                            batchSize = 0;
                        }
                    }
                    insert.executeBatch();
                }
            }
            
            stmt.executeUpdate(String.format
                    ("RENAME TABLE %1$s TO %1$s_save, %2$s TO %1$s", tableName, newTableName));
            database.setVersion(tableName, currentSchemaVersion);
            stmt.executeUpdate(String.format("DROP TABLE IF EXISTS %s_save", tableName));
        }
    }

    public void disable() {
        cache.clear();
        if (findEntry != null) {
            try { findEntry.close(); } catch (SQLException e) {}
            findEntry = null;
        }
        if (insertEntry != null) {
            try { insertEntry.close(); } catch (SQLException e) {}
            insertEntry = null;
        }
        if (updateName != null) {
            try { updateName.close(); } catch (SQLException e) {}
            updateName = null;
        }
        if (eraseStaleName != null) {
            try { eraseStaleName.close(); } catch (SQLException e) {}
            eraseStaleName = null;
        }
    }

    public int getId(UUID uuid) throws SQLException {
        synchronized (this) {
            Integer id = cache.get(uuid);
            if (id != null) {
                return id;
            }
        }

        OfflinePlayer player = plugin.getServer().getOfflinePlayer(uuid);
        return enter(uuid, player.getName());
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

        synchronized (this) {
            cache.put(uuid, id);
        }

        return id;
    }

}
