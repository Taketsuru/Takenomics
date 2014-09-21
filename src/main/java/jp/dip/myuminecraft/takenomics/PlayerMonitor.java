package jp.dip.myuminecraft.takenomics;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bukkit.OfflinePlayer;
import org.bukkit.Server;
import org.bukkit.World;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.plugin.java.JavaPlugin;

public class PlayerMonitor implements Listener {
    
    JavaPlugin                  plugin;
    Logger                      logger;
    Database                    database;
    Map<OfflinePlayer, Integer> playerIds = new HashMap<OfflinePlayer, Integer>();
    Map<World, Integer>         worldIds  = new HashMap<World, Integer>();
    PreparedStatement           joinStatement;
    PreparedStatement           quitStatement;
    PreparedStatement           playerEnterStatement;
    PreparedStatement           worldEnterStatement;

    PlayerMonitor(JavaPlugin plugin, Logger logger, Database database) {
        this.plugin = plugin;
        this.logger = logger;
        this.database = database;
    }
    
    public boolean enable() {
        Connection connection = database.getConnection();

        try {
            createTablesIfNotExist();
        } catch (SQLException e) {
            logger.warning("Failed to initialize tables: %s", e.toString());
            for (StackTraceElement t : e.getStackTrace()) {
                logger.warning(t.toString());
            }
            return false;
        }

        try {
            syncPlayersTable();
            syncWorldsTable();
        } catch (SQLException e) {
            logger.warning("Failed to synchronize tables: %s", e.toString());
            for (StackTraceElement t : e.getStackTrace()) {
                logger.warning(t.toString());
            }
            return false;
        }

        try {
            joinStatement = connection.prepareStatement
                    ("INSERT INTO sessions VALUES (NULL, NULL, ?, 'join')");
            quitStatement = connection.prepareStatement
                    ("INSERT INTO sessions VALUES (NULL, NULL, ?, 'quit')");
            playerEnterStatement = connection.prepareStatement
                    ("INSERT INTO players VALUES (NULL, ?)",
                            Statement.RETURN_GENERATED_KEYS);
            worldEnterStatement = connection.prepareStatement
                    ("INSERT INTO worlds VALUES (NULL, ?)",
                            Statement.RETURN_GENERATED_KEYS);
        } catch (SQLException e) {
            logger.warning("Failed to prepare statements: %s", e.toString());
            for (StackTraceElement t : e.getStackTrace()) {
                logger.warning(t.toString());
            }
            try {
                if (joinStatement != null) {
                    joinStatement.close();
                    joinStatement = null;
                }
                if (quitStatement != null) {
                    quitStatement.close();
                    quitStatement = null;
                }
                if (playerEnterStatement != null) {
                    playerEnterStatement.close();
                    playerEnterStatement = null;
                }
                if (worldEnterStatement != null) {
                    worldEnterStatement.close();
                    worldEnterStatement = null;
                }
            } catch (SQLException e2) {
                logger.warning("Failed to close prepared statements: %s", e2.toString());                
                for (StackTraceElement t : e.getStackTrace()) {
                    logger.warning(t.toString());
                }
            }
            return false;
        }

        plugin.getServer().getPluginManager().registerEvents(this, plugin);

        return true;
    }

    void createTablesIfNotExist() throws SQLException {
        Connection connection = database.getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS players ("
                    + "id MEDIUMINT UNSIGNED AUTO_INCREMENT NOT NULL,"
                    + "display_name VARCHAR(16) CHARACTER SET ascii NOT NULL,"
                    + "PRIMARY KEY (id)"
                    + ")");
            statement.execute("CREATE TABLE IF NOT EXISTS worlds ("
                    + "id TINYINT UNSIGNED AUTO_INCREMENT NOT NULL,"
                    + "name VARCHAR(50) NOT NULL,"
                    + "PRIMARY KEY (id)"
                    + ")");
            statement.execute("CREATE TABLE IF NOT EXISTS sessions ("
                    + "id INT UNSIGNED AUTO_INCREMENT NOT NULL,"
                    + "timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
                    + "player MEDIUMINT UNSIGNED NOT NULL,"
                    + "activity ENUM('join', 'quit') NOT NULL,"
                    + "PRIMARY KEY (id),"
                    + "INDEX (timestamp),"
                    + "FOREIGN KEY (player) REFERENCES players(id) ON DELETE CASCADE"
                    + ")");
        }
    }

    void syncWorldsTable() throws SQLException {
        Connection connection = database.getConnection();
        Server server = plugin.getServer();

        try (Statement statement = connection.createStatement()) {
            try (ResultSet result = statement.executeQuery("SELECT id, name FROM worlds")) {
                worldIds.clear();
                while (result.next()) {
                    int id = result.getInt(1);
                    String name = result.getString(2);
                    World world = server.getWorld(name);
                    if (world != null) {
                        worldIds.put(world, id);
                    }
                }
            }

            List<World> newWorlds = new ArrayList<World>();
            for (World world : plugin.getServer().getWorlds()) {
                if (! worldIds.containsKey(world)) {
                    newWorlds.add(world);
                }
            }

            if (! newWorlds.isEmpty()) {
                StringBuilder insertSql = new StringBuilder("INSERT INTO worlds VALUES");
                boolean first = true;
                for (World world : newWorlds) {
                    if (! first) {
                        insertSql.append(",");
                    } else {
                        first = false;
                    }
                    insertSql.append(String.format("(NULL, '%s')",
                            Database.escapeSingleQuotes(world.getName())));
                }

                statement.executeUpdate(insertSql.toString(), Statement.RETURN_GENERATED_KEYS);
                try (ResultSet result = statement.getGeneratedKeys()) {
                    for (World world : newWorlds) {
                        result.next();
                        worldIds.put(world, result.getInt(1));
                    }
                }
            }
        }
    }

    void syncPlayersTable() throws SQLException {
        Connection connection = database.getConnection();
        Server server = plugin.getServer();

        try (Statement statement = connection.createStatement()) {
            try (ResultSet result = statement.executeQuery("SELECT id, display_name FROM players")) {
                playerIds.clear();
                while (result.next()) {
                    int id = result.getInt(1);
                    String name = result.getString(2);
                    OfflinePlayer player = server.getOfflinePlayer(name);
                    if (player != null) {
                        playerIds.put(player, id);
                    }
                }
            }

            List<OfflinePlayer> newPlayers = new ArrayList<OfflinePlayer>();
            for (OfflinePlayer player : server.getOfflinePlayers()) {
                if (! playerIds.containsKey(player)) {
                    newPlayers.add(player);
                }
            }

            if (! newPlayers.isEmpty()) {
                StringBuilder insertSql = new StringBuilder("INSERT INTO players VALUES");
                boolean first = true;
                for (OfflinePlayer player : newPlayers) {
                    if (! first) {
                        insertSql.append(",");
                    } else {
                        first = false;
                    }
                    insertSql.append(String.format("(NULL, '%s')",
                            Database.escapeSingleQuotes(player.getName())));
                }

                statement.executeUpdate(insertSql.toString(), Statement.RETURN_GENERATED_KEYS);
                try (ResultSet result = statement.getGeneratedKeys()) {
                    for (OfflinePlayer player : newPlayers) {
                        result.next();
                        playerIds.put(player, result.getInt(1));
                    }
                }
            }
        }
    }

    @EventHandler
    public void onPlayerJoinEvent(PlayerJoinEvent event) {
        try {
            joinStatement.setInt(1, playerIds.get(event.getPlayer()));
            joinStatement.executeUpdate();
        } catch (SQLException e) {
            logger.warning("Failed to put player join entry into sessions table: %s",
                    e.toString());
            for (StackTraceElement t : e.getStackTrace()) {
                logger.warning(t.toString());
            }
        }
    }

    @EventHandler
    public void onPlayerQuitEvent(PlayerQuitEvent event) {
        try {
            quitStatement.setInt(1, playerIds.get(event.getPlayer()));
            quitStatement.executeUpdate();
        } catch (SQLException e) {
            logger.warning("Failed to put player quit entry into sessions table: %s",
                    e.toString());
            for (StackTraceElement t : e.getStackTrace()) {
                logger.warning(t.toString());
            }
        }
    }

    public int getPlayerId(OfflinePlayer player) throws SQLException {
        if (playerIds.containsKey(player)) {
            return playerIds.get(player);
        }
        
        playerEnterStatement.setString(1, player.getName());
        playerEnterStatement.executeUpdate();
      
        try (ResultSet result = playerEnterStatement.getGeneratedKeys()) {
            result.next();
            int id = result.getInt(1);
            playerIds.put(player, id);
            return id;
        }
    }
    
    public int getWorldId(World world) throws SQLException {
        if (worldIds.containsKey(world)) {
            return worldIds.get(world);
        }
        
        worldEnterStatement.setString(1, world.getName());
        worldEnterStatement.executeUpdate();
        
        try (ResultSet result = worldEnterStatement.getGeneratedKeys()) {
            result.next();
            int id = result.getInt(1);
            worldIds.put(world, id);
            return id;
        }
    }       
}
