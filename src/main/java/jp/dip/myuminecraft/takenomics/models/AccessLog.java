package jp.dip.myuminecraft.takenomics.models;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;


import jp.dip.myuminecraft.takenomics.Database;
import jp.dip.myuminecraft.takenomics.Logger;

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
        Connection connection = database.getConnection();

        try {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE IF NOT EXISTS accesses ("
                        + "id INT UNSIGNED AUTO_INCREMENT NOT NULL,"
                        + "timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,"
                        + "player MEDIUMINT UNSIGNED NOT NULL,"
                        + "activity ENUM('join', 'quit') NOT NULL,"
                        + "PRIMARY KEY (id),"
                        + "INDEX (timestamp),"
                        + "FOREIGN KEY (player) REFERENCES players(id) ON DELETE CASCADE"
                        + ")");
            }

            insertEntry = connection.prepareStatement
                    ("INSERT INTO accesses VALUES (NULL, NULL, ?, ?)");
        } catch (SQLException e) {
            logger.warning(e, "Failed to initialize access log.");
            disable();
            return false;
        }

        return true;
    }

    public void disable() {
        try {
            if (insertEntry != null) {
                insertEntry.close();
                insertEntry = null;
            }
        } catch (SQLException e) {
            logger.warning(e, "Failed to close prepared statements.");
        }
    }

    public void put(Player player, final EntryType type) {
        try {
            final int id = playerTable.getId(player);

            database.runAsynchronously(new Runnable() {
                public void run() {
                    try {
                        insertEntry.setInt(1, id);
                        insertEntry.setString(2, type.toString().toLowerCase());
                        insertEntry.executeUpdate();
                    } catch (SQLException e) {
                        logger.warning(e, "Failed to put access log entry.");
                    }
                }
            });
        } catch (SQLException e) {
            logger.warning(e, "Failed to put access log entry.");
        }
    }

}
