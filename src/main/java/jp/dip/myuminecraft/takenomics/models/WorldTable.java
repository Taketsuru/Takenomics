package jp.dip.myuminecraft.takenomics.models;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jp.dip.myuminecraft.takecore.DatabaseTask;
import jp.dip.myuminecraft.takecore.Logger;
import jp.dip.myuminecraft.takenomics.Database;

import org.bukkit.World;
import org.bukkit.plugin.java.JavaPlugin;

public class WorldTable {

    static final int     currentSchemaVersion = 0;
    static final int     maxBatchSize         = 1;
    JavaPlugin           plugin;
    Logger               logger;
    Database             database;
    String               tableName;
    Map<String, Integer> cache;
    PreparedStatement    insertEntry;
    PreparedStatement    findEntry;

    public WorldTable(JavaPlugin plugin, Logger logger, Database database) {
        this.plugin = plugin;
        this.logger = logger;
        this.database = database;
        cache = Collections.synchronizedMap(new HashMap<String, Integer>());
    }

    public String getTableName() {
        return tableName;
    }

    public boolean enable() {
        if (database == null) {
            return true;
        }

        tableName = database.getTablePrefix() + "worlds";

        List<String> worldNames = new ArrayList<>();
        for (World w : plugin.getServer().getWorlds()) {
            worldNames.add(w.getName());
        }

        try {
            database.submitSync(new DatabaseTask() {
                @Override
                public void run(Connection connection) throws Throwable {
                    initializeTable(connection, worldNames);
                }
            });
        } catch (Throwable e) {
            logger.warning(e, "Failed to initialize worlds table.");
            disable();
            return true;
        }

        return false;
    }

    void initializeTable(Connection connection, List<String> worldNames)
            throws Throwable {
        if (!database.hasTable(connection, tableName)) {
            createNewTable(connection, tableName);

        } else {
            int version = database.getVersion(connection, tableName);
            switch (version) {

            case currentSchemaVersion:
                break;

            default:
                throw new Exception(String.format(
                        "Unknown schema version: table %s, version %d",
                        tableName, version));
            }
        }

        findEntry = connection.prepareStatement(
                String.format("SELECT id FROM %s WHERE name=?", tableName));
        insertEntry = connection.prepareStatement(
                String.format("INSERT INTO %s VALUES (NULL,?)", tableName),
                Statement.RETURN_GENERATED_KEYS);

        for (String worldName : worldNames) {
            enter(connection, worldName);
        }
    }

    void createNewTable(Connection connection, String tableName)
            throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                    "CREATE TABLE %s ("
                            + "id TINYINT UNSIGNED AUTO_INCREMENT NOT NULL,"
                            + "name VARCHAR(50) CHARACTER SET ascii NOT NULL,"
                            + "PRIMARY KEY (id)," + "UNIQUE (name))",
                    tableName));

        }
    }

    public void disable() {
        cache.clear();
        if (findEntry != null) {
            try {
                findEntry.close();
            } catch (SQLException e) {
            }
            findEntry = null;
        }
        if (insertEntry != null) {
            try {
                insertEntry.close();
            } catch (SQLException e) {
            }
            insertEntry = null;
        }
    }

    public int getId(Connection connection, String name) throws SQLException {
        Integer id = cache.get(name);
        if (id != null) {
            return id;
        }

        return enter(connection, name);
    }

    public int enter(Connection connection, String name) throws SQLException {
        int id = -1;

        try {
            connection.setAutoCommit(false);

            findEntry.setString(1, name);
            try (ResultSet result = findEntry.executeQuery()) {
                if (result.next()) {
                    id = result.getInt(1);
                } else {
                    insertEntry.setString(1, name);
                    insertEntry.executeUpdate();
                    try (ResultSet updateResult = insertEntry
                            .getGeneratedKeys()) {
                        updateResult.next();
                        id = updateResult.getInt(1);
                    }
                }
            }

            connection.commit();
        } finally {
            connection.setAutoCommit(true);
        }

        cache.put(name, id);

        return id;
    }
}
