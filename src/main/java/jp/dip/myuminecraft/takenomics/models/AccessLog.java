package jp.dip.myuminecraft.takenomics.models;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.UUID;

import jp.dip.myuminecraft.takecore.DatabaseTask;
import jp.dip.myuminecraft.takecore.Logger;
import jp.dip.myuminecraft.takenomics.Database;
import jp.dip.myuminecraft.takenomics.UnknownPlayerException;

import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;

public class AccessLog {

    public enum EntryType {
        JOIN, QUIT
    }

    JavaPlugin        plugin;
    Logger            logger;
    Database          database;
    String            tableName;
    PlayerTable       playerTable;
    PreparedStatement insertEntry;

    public AccessLog(JavaPlugin plugin, Logger logger, Database database,
            PlayerTable playerTable) {
        this.plugin = plugin;
        this.logger = logger;
        this.database = database;
        this.playerTable = playerTable;
    }

    public boolean enable() {
        if (database == null || playerTable == null) {
            return true;
        }

        try {
            database.submitSync(new DatabaseTask() {
                @Override
                public void run(Connection connection) throws Throwable {
                    tableName = database.getTablePrefix() + "accesses";

                    String createTable = String.format(
                            "CREATE TABLE IF NOT EXISTS %s ("
                                    + "id INT UNSIGNED AUTO_INCREMENT NOT NULL,"
                                    + "timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,"
                                    + "player MEDIUMINT UNSIGNED NOT NULL,"
                                    + "activity ENUM('join', 'quit') NOT NULL,"
                                    + "PRIMARY KEY (id),"
                                    + "INDEX (timestamp),"
                                    + "FOREIGN KEY (player) REFERENCES %s(id) ON DELETE CASCADE)",
                            tableName, playerTable.getTableName());

                    try (Statement statement = connection.createStatement()) {
                        statement.execute(createTable);
                    }

                    insertEntry = connection.prepareStatement(String.format(
                            "INSERT INTO %s VALUES (NULL, NULL, ?, ?)",
                            tableName));
                }
            });

        } catch (Throwable e) {
            logger.warning(e, "Failed to initialize access log.");
            disable();
            return true;
        }

        return false;
    }

    public void disable() {
        database.submitAsync(new DatabaseTask() {
            @Override
            public void run(Connection connection) throws Throwable {
                if (insertEntry != null) {
                    insertEntry.close();
                    insertEntry = null;
                }
            }
        });
    }

    public void put(Player player, EntryType type) {
        UUID uuid = player.getUniqueId();

        database.submitAsync(new DatabaseTask() {
            public void run(Connection connection) throws Throwable {
                try {
                    insertEntry.setInt(1, playerTable.getId(connection, uuid));
                    insertEntry.setString(2, type.toString().toLowerCase());
                    insertEntry.executeUpdate();
                } catch (SQLException | UnknownPlayerException e) {
                    logger.warning(e, "Failed to put access log entry.");
                }
            }
        });
    }

}
