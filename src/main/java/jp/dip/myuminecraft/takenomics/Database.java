package jp.dip.myuminecraft.takenomics;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.plugin.java.JavaPlugin;

import jp.dip.myuminecraft.takecore.DatabaseTask;
import jp.dip.myuminecraft.takecore.Logger;

public class Database extends jp.dip.myuminecraft.takecore.Database {

    static final String  configEnable      = "enable";
    static final String  configDebug       = "debug";
    static final String  configUrl         = "url";
    static final String  configUser        = "user";
    static final String  configPassword    = "password";
    static final String  configTablePrefix = "tablePrefix";
    static final Pattern tableNamePattern  = Pattern
            .compile("[a-zA-Z0-9_][a-zA-Z0-9_]*");

    JavaPlugin           plugin;
    Logger               logger;
    PreparedStatement    checkTable;
    PreparedStatement    queryVersion;
    PreparedStatement    updateVersion;
    PreparedStatement    insertVersion;
    String               databaseName;
    String               tablePrefix;
    boolean              debug;

    public Database(JavaPlugin plugin, Logger logger) {
        super(plugin, logger);
        this.plugin = plugin;
        this.logger = logger;
    }

    public boolean enable(ConfigurationSection config) {
        String configPath = config.getCurrentPath();

        if (!config.contains(configEnable)) {
            return true;
        }

        if (!config.isBoolean(configEnable)) {
            logger.warning("'%s.%s' is not a boolean.", configPath,
                    configEnable);
            return true;
        }

        if (!config.getBoolean(configEnable)) {
            return true;
        }

        if (!config.contains(configUrl)) {
            logger.warning("'%s.%s' is not specified.", configPath, configUrl);
            return true;
        }

        if (!config.contains(configDebug) || !config.contains(configDebug)) {
            debug = false;
        } else if (!config.isBoolean(configDebug)) {
            logger.warning("'%s.%s' is not a boolean.", configPath,
                    configDebug);
            debug = false;
        } else {
            debug = config.getBoolean(configDebug);
        }

        Properties connectionProperties = new Properties();

        if (config.contains(configUser)) {
            connectionProperties.put("user", config.getString(configUser));
        }

        if (config.contains(configPassword)) {
            connectionProperties.put("password",
                    config.getString(configPassword));
        }

        if (!config.contains(configTablePrefix)) {
            tablePrefix = "takenomics_";
        } else {
            tablePrefix = config.getString(configTablePrefix);
            if (!tableNamePattern.matcher(tablePrefix).matches()) {
                logger.warning("table prefix '%s' is not valid", tablePrefix);
                return true;
            }
        }

        try {
            super.enable(config.getString("url"), connectionProperties);
            submitSync(new DatabaseTask() {
                @Override
                public void run(Connection connection) throws Throwable {
                    try (Statement stmt = connection.createStatement()) {
                        ResultSet rs = stmt.executeQuery("SELECT DATABASE()");
                        rs.next();
                        databaseName = rs.getString(1);
                        if (databaseName == null) {
                            throw new Exception(String.format(
                                    "%s.%s doesn't specify database name",
                                    configPath, configUrl));
                        }
                    }
                    initializeStatements(connection);
                }
            });
        } catch (Throwable e) {
            logger.warning(e, "Failed to initialize database connection.");
            disable();
            return true;
        }

        return false;
    }

    void initializeStatements(Connection connection) throws SQLException {
        checkTable = connection.prepareStatement(String.format(
                "SELECT table_name FROM information_schema.tables "
                        + "WHERE table_schema = '%s' AND table_name = ?",
                databaseName));
        String versionTableName = tablePrefix + "version";

        try (Statement createTable = connection.createStatement()) {
            createTable.execute(
                    String.format(
                            "CREATE TABLE IF NOT EXISTS %s ("
                                    + "table_name VARCHAR(63) CHARACTER SET ascii NOT NULL,"
                                    + "schema_version TINYINT UNSIGNED NOT NULL,"
                                    + "PRIMARY KEY (table_name))",
                            versionTableName));
        }

        queryVersion = connection.prepareStatement(String.format(
                "SELECT schema_version FROM %s WHERE table_name=?",
                versionTableName));

        updateVersion = connection.prepareStatement(String.format(
                "UPDATE %s SET schema_version=? WHERE table_name=?",
                versionTableName));

        insertVersion = connection.prepareStatement(String
                .format("INSERT INTO %s VALUES (?,?)", versionTableName));
    }

    public void disable() {
        try {
            submitSync(new DatabaseTask() {
                @Override
                public void run(Connection connection) throws Throwable {
                    if (insertVersion != null) {
                        insertVersion.close();
                        insertVersion = null;
                    }

                    if (updateVersion != null) {
                        updateVersion.close();
                        updateVersion = null;
                    }

                    if (queryVersion != null) {
                        queryVersion.close();
                        queryVersion = null;
                    }

                    if (checkTable != null) {
                        checkTable.close();
                        checkTable = null;
                    }
                }
            });
        } catch (Throwable t) {
            logger.warning(t, "Failed to disable Database.");
        }

        super.disable();
        tablePrefix = null;
    }

    public String getTablePrefix() {
        return tablePrefix;
    }

    public boolean hasTable(Connection connection, String tableName)
            throws SQLException {
        checkTable.setString(1, tableName);
        try (ResultSet rs = checkTable.executeQuery()) {
            return rs.next();
        }
    }

    public int getVersion(Connection connection, String tableName)
            throws SQLException {
        queryVersion.setString(1, tableName);
        try (ResultSet rs = queryVersion.executeQuery()) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    public void setVersion(Connection connection, String tableName,
            int version) throws SQLException {
        if (getVersion(connection, tableName) == 0) {
            insertVersion.setString(1, tableName);
            insertVersion.setInt(2, version);
            insertVersion.executeUpdate();
        } else {
            updateVersion.setInt(1, version);
            updateVersion.setString(2, tableName);
            updateVersion.executeUpdate();
        }
    }

    public static void toBytes(UUID uuid, byte[] result) {
        long high = uuid.getMostSignificantBits();
        long low = uuid.getLeastSignificantBits();
        for (int i = 0; i < 8; ++i) {
            result[i] = (byte) ((high >>> (56 - i * 8)) & 0xff);
            result[i + 8] = (byte) ((low >>> (56 - i * 8)) & 0xff);
        }
    }

    public static UUID toUUID(byte[] uuidBytes) {
        long high = 0;
        long low = 0;
        for (int i = 0; i < 8; ++i) {
            high |= (((long) uuidBytes[i] & 0xff) << (56 - i * 8));
            low |= (((long) uuidBytes[i + 8] & 0xff) << (56 - i * 8));
        }
        return new UUID(high, low);
    }

}
