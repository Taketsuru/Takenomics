package jp.dip.myuminecraft.takenomics.models;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import jp.dip.myuminecraft.takecore.Logger;
import jp.dip.myuminecraft.takenomics.Database;
import jp.dip.myuminecraft.takenomics.UnknownPlayerException;
import jp.dip.myuminecraft.takenomics.models.ShopTable.Shop;

import org.bukkit.plugin.java.JavaPlugin;

import com.Acrobot.ChestShop.Events.TransactionEvent.TransactionType;

public class TransactionTable {

    JavaPlugin        plugin;
    Logger            logger;
    Database          database;
    PlayerTable       playerTable;
    ShopTable         shopTable;
    String            tableName;
    PreparedStatement insertTransaction;

    public TransactionTable(JavaPlugin plugin, Logger logger, Database database,
            PlayerTable playerTable, ShopTable shopTable) {
        this.plugin = plugin;
        this.logger = logger;
        this.database = database;
        this.playerTable = playerTable;
        this.shopTable = shopTable;
    }
    
    public String getTableName() {
        assert tableName != null;
        return tableName;
    }

    public boolean enable() {
        if (database == null) {
            return true;
        }

        String tablePrefix = database.getTablePrefix();
        tableName = tablePrefix + "transactions";

        try {
            setupTable();
            prepareStatements();
        } catch (SQLException e) {
            logger.warning(e, "Failed to setup transaction table.");
            disable();
            return true;
        }
        
        return false;
    }
    
    public void disable() {
        if (insertTransaction != null) {
            try { insertTransaction.close(); } catch (SQLException e) {}
            insertTransaction = null;
        }

        tableName = null;
    }

    public void put(Shop shop, UUID player, TransactionType type, int amount)
            throws SQLException, UnknownPlayerException {
        Connection connection = database.getConnection();

        shopTable.put(shop);

        connection.setAutoCommit(false);
        try {
            int shopId = shopTable.getId(shop.world, shop.x, shop.y, shop.z);
            insertTransaction.setInt(1, shopId);
            int playerId = playerTable.getId(player);
            insertTransaction.setInt(2, playerId);
            insertTransaction.setString(3, type.toString().toLowerCase());
            insertTransaction.setInt(4, amount);
            insertTransaction.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    void setupTable() throws SQLException {
        Connection connection = database.getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format("CREATE TABLE IF NOT EXISTS %s ("
                    + "id INT UNSIGNED AUTO_INCREMENT NOT NULL,"
                    + "timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,"
                    + "shop SMALLINT UNSIGNED NOT NULL,"
                    + "player MEDIUMINT UNSIGNED NOT NULL,"
                    + "type ENUM('buy', 'sell') NOT NULL,"
                    + "quantity SMALLINT NOT NULL," + "PRIMARY KEY (id),"
                    + "FOREIGN KEY (shop) REFERENCES %s (id),"
                    + "FOREIGN KEY (player) REFERENCES %s (id)" + ")",
                    tableName,
                    shopTable.getTableName(),
                    playerTable.getTableName()));
        }
    }

    void prepareStatements() throws SQLException {
        Connection connection = database.getConnection();

        insertTransaction = connection.prepareStatement(String.format(
                "INSERT INTO %s VALUES (NULL,NULL,?,?,?,?)",
                tableName));
    }

}
