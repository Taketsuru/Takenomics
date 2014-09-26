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

    JavaPlugin           plugin;
    Logger               logger;
    Database             database;
    String               tableName;
    Map<String, Integer> cache = new HashMap<String, Integer>();
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

            try (PreparedStatement stmt = connection.prepareStatement
                    ("SELECT table_name FROM information_schema.tables "
                            + "WHERE table_schema = ? AND table_name = ?")) {
                stmt.setString(1, database.getDatabaseName());
                stmt.setString(2, tableName);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (! rs.next()) {
                        createTable(tableName);
                    }
                }
            }

            findEntry = connection.prepareStatement
                    (String.format("SELECT id FROM %s WHERE name=?", tableName));
            insertEntry = connection.prepareStatement
                    (String.format("INSERT INTO %s VALUES (NULL,?)", tableName),
                            Statement.RETURN_GENERATED_KEYS);

        } catch (SQLException e) {
            logger.warning(e, "Failed to initialize worlds table.");
            disable();
            return false;
        }

        return true;
    }

    void createTable(String tableName) throws SQLException {
        Connection connection = database.getConnection();

        String idType = "TINYINT UNSIGNED AUTO_INCREMENT NOT NULL";
        String nameType = "VARCHAR(50) CHARACTER SET ascii NOT NULL";

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format
                    ("CREATE TABLE %s ("
                            + "id %s,"
                            + "name %s,"
                            + "PRIMARY KEY (id),"
                            + "UNIQUE (name))",
                            tableName, idType, nameType));

            stmt.execute(String.format
                    ("CREATE TEMPORARY TABLE IF NOT EXISTS %s_load ("
                            + "name %s"
                            + ") ENGINE=MEMORY",
                            tableName, nameType));

            try (PreparedStatement insert =
                    connection.prepareStatement
                    (String.format("INSERT INTO %s_load VALUES (?)", tableName))) {
                for (World world : plugin.getServer().getWorlds()) {
                    insert.setString(1, world.getName());
                    insert.addBatch();
                }
                insert.executeBatch();
            }

            stmt.execute(String.format
                    ("INSERT INTO %1$s "
                            + "SELECT NULL, %1$s_load.name "
                            + "FROM %1$s_load LEFT JOIN %1$s "
                            + "ON %1$s_load.name = %1$s.name "
                            + "WHERE %1$s.id IS NULL", tableName));

            try (ResultSet result = stmt.executeQuery(String.format
                    ("SELECT %1$s.id, %1$s.name "
                            + "FROM %1$s INNER JOIN %1$s_load "
                            + "ON %1$s.name = %1$s_load.name "
                            + "WHERE %1$s_load.name IS NOT NULL", tableName))) {
                while (result.next()) {
                    cache.put(result.getString(2), result.getInt(1));
                }
            }

            stmt.execute(String.format("DROP TABLE %s_load", tableName));
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
        } catch (SQLException e) {
            logger.warning(e, "Failed to close prepared statements.");
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
