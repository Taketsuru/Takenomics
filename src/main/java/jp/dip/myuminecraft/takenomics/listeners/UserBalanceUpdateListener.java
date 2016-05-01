package jp.dip.myuminecraft.takenomics.listeners;

import java.sql.Connection;

import jp.dip.myuminecraft.takecore.DatabaseTask;
import jp.dip.myuminecraft.takecore.Logger;
import jp.dip.myuminecraft.takenomics.Database;
import jp.dip.myuminecraft.takenomics.models.PlayerTable;
import net.ess3.api.events.UserBalanceUpdateEvent;

import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.plugin.java.JavaPlugin;

public class UserBalanceUpdateListener implements Listener {

    JavaPlugin  plugin;
    Logger      logger;
    Database    database;
    PlayerTable playerTable;

    public UserBalanceUpdateListener(JavaPlugin plugin, Logger logger,
            Database database, PlayerTable playerTable) {
        this.plugin = plugin;
        this.logger = logger;
        this.database = database;
        this.playerTable = playerTable;
    }

    @EventHandler
    public void onUserBalanceUpdate(UserBalanceUpdateEvent event) {
        if (database == null || playerTable == null) {
            return;
        }

        // Since UserBalanceUpdateEvent returns corrupted Player for offline
        // players, and only valid info of the object is player's name,
        // the following code use Player.getName() instead of getUniqueId().
        String name = event.getPlayer().getName();
        double balance = event.getNewBalance().doubleValue();

        database.submitAsync(new DatabaseTask() {
            @Override
            public void run(Connection connection) throws Throwable {
                playerTable.updateBalance(connection, name, balance);
            }
        });
    }

}
