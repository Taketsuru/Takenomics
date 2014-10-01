package jp.dip.myuminecraft.takenomics.models;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import jp.dip.myuminecraft.takenomics.Database;
import jp.dip.myuminecraft.takenomics.Logger;

import org.bukkit.World;
import org.bukkit.plugin.java.JavaPlugin;

public class WorldTable {

    static final int     currentSchemaVersion = 0;
    static final int     maxBatchSize         = 1;
    JavaPlugin           plugin;
    Logger               logger;
    Database             database;
    String               tableName;
    Map<String, Integer> cache                = new HashMap<String, Integer>();
    PreparedStatement    insertEntry;
    PreparedStatement    findEntry;

    public WorldTable(JavaPlugin plugin, Logger logger, Database database) {
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

        tableName = database.getTablePrefix() + "worlds";
        Connection connection = database.getConnection();            

        try {

            if (! database.hasTable(tableName)) {
                createNewTable(tableName);
            } else {
                int version = database.getVersion(tableName);
                if (currentSchemaVersion < version) {
                    logger.warning("The schema version of %s is higher (%d) "
                            + "than the current version (%d).",
                            tableName, version, currentSchemaVersion);
                    disable();
                    return false;
                }
            }
            
            findEntry = connection.prepareStatement
                    (String.format("SELECT id FROM %s WHERE name=?", tableName));
            insertEntry = connection.prepareStatement
                    (String.format("INSERT INTO %s VALUES (NULL,?)", tableName),
                            Statement.RETURN_GENERATED_KEYS);

            enterLoadedWorlds();

        } catch (SQLException e) {
            logger.warning(e, "Failed to initialize worlds table.");
            disable();
            return false;
        }

        return true;
    }

    void createNewTable(String tableName) throws SQLException {
        Connection connection = database.getConnection();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format
                    ("CREATE TABLE %s ("
                            + "id TINYINT UNSIGNED AUTO_INCREMENT NOT NULL,"
                            + "name VARCHAR(50) CHARACTER SET ascii NOT NULL,"
                            + "PRIMARY KEY (id),"
                            + "UNIQUE (name))",
                            tableName));

        }
    }

    void enterLoadedWorlds() throws SQLException {
        for (World w : plugin.getServer().getWorlds()) {
            enter(w.getName());
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
    }

    public synchronized int getId(String name) throws SQLException {
        synchronized (this) {
            Integer id = cache.get(name);
            if (id != null) {
                return id;
            }
        }

        return enter(name);
    }

    public int enter(String name) throws SQLException {
        int id = -1;
        Connection connection = database.getConnection();

        try {
            connection.setAutoCommit(false);

            findEntry.setString(1, name);
            try (ResultSet result = findEntry.executeQuery()) {
                if (result.next()) {
                    id = result.getInt(1);
                } else {
                    insertEntry.setString(1, name);
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
            cache.put(name, id);
        }

        return id;
    }
}
