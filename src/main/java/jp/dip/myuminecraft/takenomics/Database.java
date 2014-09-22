package jp.dip.myuminecraft.takenomics;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.plugin.java.JavaPlugin;

public class Database {

    JavaPlugin plugin;
    Logger     logger;
    Connection connection;
    JobQueue   queue;

    public Database(JavaPlugin plugin, Logger logger) {
        this.plugin = plugin;
        this.logger = logger;
        queue = new JobQueue(plugin);
    }

    public boolean enable() {
        String configPrefix = "database";
        String configEnable = configPrefix + ".enable";
        String configHost = configPrefix + ".host";
        String configPort = configPrefix + ".port";
        String configDatabase = configPrefix + ".database";
        String configUser = configPrefix + ".user";
        String configPassword = configPrefix + ".password";

        FileConfiguration config = plugin.getConfig();
        
        connection = null;
        if (! config.contains(configPrefix) || ! config.contains(configEnable)) {
            return true;
        }

        if (! config.isBoolean(configEnable)) {
            logger.warning("'%s' is not a boolean.", configEnable);
            return false;
        }

        if (! config.getBoolean(configEnable)) {
            return true;
        }
        
        if (! config.contains(configHost)) {
            logger.warning("'%s' is not configured.", configHost);
            return false;
        }

        if (! config.contains(configDatabase)) {
            logger.warning("%s is not configured.", configDatabase);
            return false;
        }

        Properties connectionProperties = new Properties();

        if (config.contains(configUser)) {
            connectionProperties.put("user", config.getString(configUser));
        }

        if (config.contains(configPassword)) {
            connectionProperties.put("password", config.getString(configPassword));
        }

        String host = config.getString(configHost);
        String port = config.contains(configPort)
                ? config.getString(configPort)
                : "3306";
        String database = config.getString(configDatabase);
        String url = String.format("jdbc:mysql://%s:%s/%s", host, port, database);

        try {
            connection = DriverManager.getConnection(url, connectionProperties);
        } catch (SQLException e) {
            logger.warning("Failed to connect to %s.  %s", url, e.getMessage());
            return false;
        }
        
        return true;
    }
    
    public void disable() {
        queue.drain();

        if (connection == null) {
            return;
        }

        try {
            connection.close();
        } catch (SQLException e) {
            logger.warning(e, "Failed to close connection.");            
        }
        connection = null;
    }
    
    public Connection getConnection() {
        return connection;
    }

    public static String escapeSingleQuotes(String name) {
        int to = name.indexOf('\'');
        if (to == -1) {
            return name;
        }

        StringBuilder result = new StringBuilder();
        int from = 0;
        do {
            result.append(name.substring(from, to));
            result.append("''");
            from = to + 1;
            to = name.indexOf('\'', from);
        } while (to != -1);
        result.append(name.substring(from));
        
        return result.toString();
    }

    public void runAsynchronously(Runnable runnable) {
        queue.runAsynchronously(runnable);
    }

}
