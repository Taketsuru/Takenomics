package jp.dip.myuminecraft.takenomics.models;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;


import java.util.UUID;

import jp.dip.myuminecraft.takenomics.Database;
import jp.dip.myuminecraft.takenomics.Logger;
import jp.dip.myuminecraft.takenomics.UnknownPlayerException;

import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;

public class AccessLog {

    public enum EntryType {
        JOIN,
        QUIT
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

        Connection connection = database.getConnection();
        tableName = database.getTablePrefix() + "accesses";

        try {
            try (Statement statement = connection.createStatement()) {
                statement.execute(String.format
                        ("CREATE TABLE IF NOT EXISTS %s ("
                        + "id INT UNSIGNED AUTO_INCREMENT NOT NULL,"
                        + "timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,"
                        + "player MEDIUMINT UNSIGNED NOT NULL,"
                        + "activity ENUM('join', 'quit') NOT NULL,"
                        + "PRIMARY KEY (id),"
                        + "INDEX (timestamp),"
                        + "FOREIGN KEY (player) REFERENCES %s(id) ON DELETE CASCADE"
                        + ")", tableName, playerTable.getTableName()));
            }

            insertEntry = connection.prepareStatement
                    (String.format("INSERT INTO %s VALUES (NULL, NULL, ?, ?)", tableName));
        } catch (SQLException e) {
            logger.warning(e, "Failed to initialize access log.");
            disable();
            return true;
        }

        return false;
    }

    public void disable() {
        if (insertEntry != null) {
            try { insertEntry.close(); } catch (SQLException e) {}
            insertEntry = null;
        }
    }

    public void put(Player player, final EntryType type) {
        final UUID uuid = player.getUniqueId();

        database.runAsynchronously(new Runnable() {
            public void run() {
                try {
                    insertEntry.setInt(1, playerTable.getId(uuid));
                    insertEntry.setString(2, type.toString().toLowerCase());
                    insertEntry.executeUpdate();
                } catch (SQLException | UnknownPlayerException e) {
                    logger.warning(e, "Failed to put access log entry.");
                }
            }
        });
    }

}
