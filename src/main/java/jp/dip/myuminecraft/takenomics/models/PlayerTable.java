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
import org.bukkit.plugin.java.JavaPlugin;

public class PlayerTable {

    JavaPlugin         plugin;
    Logger             logger;
    Database           database;
    String             tableName;
    Map<UUID, Integer> cache = new HashMap<UUID, Integer>();
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

        tableName = database.getTablePrefix() + "players";

        Connection connection = database.getConnection();            
        try {
            try (PreparedStatement checkTable = connection.prepareStatement
                    ("SELECT table_name FROM information_schema.tables "
                            + "WHERE table_schema = ? AND table_name = ?")) {
                checkTable.setString(1, database.getDatabaseName());
                checkTable.setString(2, tableName);
                try (ResultSet rs = checkTable.executeQuery()) {
                    if (! rs.next()) {
                        createTable();
                    }
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
            logger.warning(e, "Failed to initialize PlayerTable.");
            disable();
            return false;
        }
        
        return true;
    }

    private void createTable() throws SQLException {
        String uuidType = "BINARY(16) NOT NULL";
        String nameType = "VARCHAR(16) CHARACTER SET ascii";

        Connection connection = database.getConnection();

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format
                    ("CREATE TABLE %s ("
                            + "id MEDIUMINT UNSIGNED AUTO_INCREMENT NOT NULL,"
                            + "uuid %s,"
                            + "name %s,"
                            + "PRIMARY KEY (id),"
                            + "UNIQUE (uuid),"
                            + "UNIQUE (name))",
                            tableName, uuidType, nameType));

            try (PreparedStatement insert =
                    connection.prepareStatement
                    (String.format("INSERT INTO %s VALUES (NULL,?,?)", tableName))) {
                byte[] uuid = new byte[Constants.sizeOfUUID];
                for (OfflinePlayer player : plugin.getServer().getOfflinePlayers()) {
                    Database.toBytes(player.getUniqueId(), uuid);
                    insert.setBytes(1, uuid);
                    insert.setString(2, player.getName());
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
    }

    public void disable() {
        cache.clear();
        try {
            if (findEntry != null) {
                findEntry.close();
                findEntry = null;
            }
            if (insertEntry != null) {
                insertEntry.close();
                insertEntry = null;
            }
            if (updateName != null) {
                updateName.close();
                updateName = null;
            }
        } catch (SQLException e) {
            logger.warning(e, "Failed to close prepared statements.");
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
        return enter(player.getUniqueId(), player.getName());
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
