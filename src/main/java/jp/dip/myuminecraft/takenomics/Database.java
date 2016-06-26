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

import jp.dip.myuminecraft.takecore.DatabaseTask;
import jp.dip.myuminecraft.takecore.Logger;
import jp.dip.myuminecraft.takecore.MisconfigurationException;

public class Database extends jp.dip.myuminecraft.takecore.Database {

    private static final String  configUrl         = "url";
    private static final String  configUser        = "user";
    private static final String  configPassword    = "password";
    private static final String  configTablePrefix = "tablePrefix";
    private static final Pattern tableNamePattern  = Pattern
            .compile("[a-zA-Z0-9_][a-zA-Z0-9_]*");
    private String               databaseName;
    private String               tablePrefix;
    private String               versionTableName;

    public Database(Logger logger) {
        super(logger);
    }

    public void enable(ConfigurationSection config)
            throws MisconfigurationException, SQLException {
        String configPath = config.getCurrentPath();

        if (!config.contains(configUrl)) {
            throw new MisconfigurationException(String.format(
                    "'%s.%s' is not specified.", configPath, configUrl));
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
                throw new MisconfigurationException(String.format(
                        "table prefix '%s' is not valid", tablePrefix));
            }
        }

        super.enable(config.getString("url"), connectionProperties);

        versionTableName = tablePrefix + "version";

        try {
            submitSync(new DatabaseTask() {
                @Override
                public void run(Connection connection)
                        throws SQLException, MisconfigurationException {

                    try (Statement stmt = connection.createStatement()) {
                        ResultSet rs = stmt.executeQuery("SELECT DATABASE()");
                        if (!rs.next()
                                || (databaseName = rs.getString(1)) == null) {
                            throw new MisconfigurationException(String.format(
                                    "%s.%s doesn't specify database name",
                                    configPath, configUrl));
                        }

                        stmt.execute(String.format(
                                "CREATE TABLE IF NOT EXISTS %s ("
                                        + "table_name VARCHAR(63) CHARACTER SET ascii NOT NULL PRIMARY KEY,"
                                        + "schema_version TINYINT UNSIGNED NOT NULL)",
                                versionTableName));
                    }
                }
            });
        } catch (SQLException | MisconfigurationException e) {
            throw e;

        } catch (Throwable e) {
            throw new AssertionError("Unexpected exception", e);
        }
    }

    public void disable() {
        versionTableName = null;
        tablePrefix = null;
        super.disable();
    }

    public String getTablePrefix() {
        return tablePrefix;
    }

    public boolean hasTable(Connection connection, String tableName)
            throws SQLException {
        try (PreparedStatement checkTable = connection.prepareStatement(
                "SELECT table_name " + "FROM information_schema.tables "
                        + "WHERE table_schema = ? AND table_name = ?")) {
            checkTable.setString(1, databaseName);
            checkTable.setString(2, tableName);
            return checkTable.executeQuery().next();
        }
    }

    public int getVersion(Connection connection, String tableName)
            throws SQLException {
        try (PreparedStatement queryVersion = connection
                .prepareStatement(String.format(
                        "SELECT schema_version FROM %s WHERE table_name = ?",
                        versionTableName))) {
            queryVersion.setString(1, tableName);
            ResultSet rs = queryVersion.executeQuery();
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    public void setVersion(Connection connection, String tableName,
            int version) throws SQLException {
        try (PreparedStatement insertVersion = connection
                .prepareStatement(String.format(
                        "INSERT INTO %s VALUES (?,?) "
                                + "ON DUPLICATE KEY UPDATE schema_version = ?",
                        versionTableName))) {
            insertVersion.setString(1, tableName);
            insertVersion.setInt(2, version);
            insertVersion.setInt(3, version);
            insertVersion.executeUpdate();
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
