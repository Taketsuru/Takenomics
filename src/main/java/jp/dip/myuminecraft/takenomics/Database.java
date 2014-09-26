package jp.dip.myuminecraft.takenomics;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;

import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.plugin.java.JavaPlugin;

public class Database {

    static final String configPrefix      = "database";
    static final String configEnable      = configPrefix + ".enable";
    static final String configDebug       = configPrefix + ".debug";
    static final String configHost        = configPrefix + ".host";
    static final String configPort        = configPrefix + ".port";
    static final String configDatabase    = configPrefix + ".database";
    static final String configUser        = configPrefix + ".user";
    static final String configPassword    = configPrefix + ".password";
    static final String configTablePrefix = configPrefix + ".tablePrefix";
    JavaPlugin          plugin;
    Logger              logger;
    Connection          connection;
    JobQueue            queue;
    String              database;
    String              tablePrefix;
    boolean             debug;

    public Database(JavaPlugin plugin, Logger logger) {
        this.plugin = plugin;
        this.logger = logger;
        queue = new JobQueue(plugin, logger);
    }

    public boolean enable() {
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

        if (! config.contains(configDebug) || ! config.contains(configDebug)) {
            debug = false;
        } else if (! config.isBoolean(configDebug)) {
            logger.warning("'%s' is not a boolean.", configDebug);
            debug = false;
        } else {
            debug = config.getBoolean(configDebug);
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
        
        if (! config.contains(configTablePrefix)) {
            tablePrefix = "takenomics_";
        } else {
            tablePrefix = config.getString(configTablePrefix);
        }

        String host = config.getString(configHost);
        String port = config.contains(configPort)
                ? config.getString(configPort)
                : "3306";
        database = config.getString(configDatabase);
        String url = String.format("jdbc:mysql://%s:%s/%s", host, port, database);

        if (debug) {
            logger.info("%s: '%s'", configHost, host);
            logger.info("%s: '%s'", configPort, port);
            logger.info("%s: '%s'", configDatabase, database);
            logger.info("%s: '%s'", configUser, connectionProperties.get("user"));
            logger.info("%s: '%s'", configPassword, connectionProperties.get("password"));
            logger.info("%s: '%s'", configTablePrefix, tablePrefix);
        }

        try {
            connection = DriverManager.getConnection(url, connectionProperties);
        } catch (SQLException e) {
            logger.warning(e, "Failed to connect to %s.", url);
            return false;
        }
        
        return true;
    }
    
    public void disable() {
        queue.drain();

        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.warning(e, "Failed to close connection.");            
            }
            connection = null;
        }
        
        database = null;
        tablePrefix = null;
    }

    public String getTablePrefix() {
        return tablePrefix;
    }

    public String getDatabaseName() {
        return database;
    }

    public Connection getConnection() {
        return connection;
    }
    
    public void runAsynchronously(Runnable runnable) {
        queue.runAsynchronously(runnable);
    }

    public static void toBytes(UUID uuid, byte[] result) {
        long high = uuid.getMostSignificantBits();
        long low  = uuid.getLeastSignificantBits();
        for (int i = 0; i < 8; ++i) {
            result[i] = (byte)(high >>> (64 - i * 8));
            result[i + 8] = (byte)(low >>> (64 - i * 8));
        }
    }

    public static UUID toUUID(byte[] uuidBytes) {
        long high = 0;
        long low  = 0;
        for (int i = 0; i < 8; ++i) {
            high |= ((long)uuidBytes[i] << (64 - i * 8));
            low |= ((long)uuidBytes[i + 8] << (64 - i * 8));
        }
        return new UUID(high, low);
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

}
