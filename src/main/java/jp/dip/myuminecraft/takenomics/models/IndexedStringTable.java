package jp.dip.myuminecraft.takenomics.models;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import jp.dip.myuminecraft.takenomics.Database;
import jp.dip.myuminecraft.takenomics.Logger;

import org.bukkit.plugin.java.JavaPlugin;

public class IndexedStringTable {

    JavaPlugin           plugin;
    Logger               logger;
    Database             database;
    String               tableName;
    String               idType;
    String               nameType;
    Map<String, Integer> idTable = new HashMap<String, Integer>();
    PreparedStatement    insertEntry;

    public IndexedStringTable(JavaPlugin plugin, Logger logger, Database database,
            String tableName, String idType, String nameType) {
        this.plugin = plugin;
        this.logger = logger;
        this.database = database;
        this.tableName = tableName;
        this.idType = idType;
        this.nameType = nameType;
    }
        
    public void enable(Iterator<String> keyIter) throws SQLException {
        Connection connection = database.getConnection();            

        try {
            try (Statement stmt = connection.createStatement()) {

                connection.setAutoCommit(false);

                stmt.execute(String.format
                        ("CREATE TABLE IF NOT EXISTS %s ("
                                + "id %s UNSIGNED AUTO_INCREMENT NOT NULL,"
                                + "name %s NOT NULL,"
                                + "PRIMARY KEY (id)"
                                + ")",
                                tableName, idType, nameType));

                stmt.execute(String.format
                        ("CREATE TEMPORARY TABLE IF NOT EXISTS %s_load ("
                                + "name %s NOT NULL"
                                + ") ENGINE=MEMORY",
                                tableName, nameType));

                stmt.execute(String.format("TRUNCATE TABLE %s_load", tableName));

                try (PreparedStatement insert =
                        connection.prepareStatement
                        (String.format("INSERT INTO %s_load VALUES (?)", tableName))) {
                    while (keyIter.hasNext()) {
                        insert.setString(1, keyIter.next());
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
                        idTable.put(result.getString(2), result.getInt(1));
                    }
                }

                stmt.execute(String.format("TRUNCATE TABLE %s_load", tableName));
            }

            insertEntry = connection.prepareStatement
                    (String.format("INSERT INTO %s VALUES (NULL,?)", tableName),
                            Statement.RETURN_GENERATED_KEYS);

            connection.commit();

        } finally {
            connection.setAutoCommit(true);
        }
    }

    public void disable() {
        idTable.clear();
        try {
            if (insertEntry != null) {
                insertEntry.close();
                insertEntry = null;
            }
        } catch (SQLException e) {
            logger.warning(e, "Failed to close prepared statements.");
        }
    }

    // This member function can be called asynchronously from AsyncPlayerPreLoginEvent handler.
    public synchronized int getId(String key) throws SQLException {
        Integer value = idTable.get(key);
        if (value != null) {
            return value;
        }

        insertEntry.setString(1, key);
        insertEntry.executeUpdate();

        try (ResultSet result = insertEntry.getGeneratedKeys()) {
            result.next();
            int id = result.getInt(1);
            idTable.put(key, id);
            return id;
        }
    }

    public synchronized void mayDropCache(String name) {
        idTable.remove(name);
    }       
}
