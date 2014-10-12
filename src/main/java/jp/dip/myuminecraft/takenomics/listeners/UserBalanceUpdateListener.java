package jp.dip.myuminecraft.takenomics.listeners;

import java.sql.SQLException;
import java.util.UUID;

import jp.dip.myuminecraft.takenomics.Database;
import jp.dip.myuminecraft.takenomics.Logger;
import jp.dip.myuminecraft.takenomics.UnknownPlayerException;
import jp.dip.myuminecraft.takenomics.models.PlayerTable;
import net.ess3.api.events.UserBalanceUpdateEvent;

import org.bukkit.OfflinePlayer;
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

        // Since UserBalanceUpdateEvent returns corrupted Player for offline players,
        // and only valid info of the object is player's name,
        // the following code find the UUID from the name.
        final String name = event.getPlayer().getName();
        final double balance = event.getNewBalance().doubleValue();

        database.runAsynchronously(new Runnable() {
            public void run() {
                try {
                    UUID uuid = playerTable.getUniqueIdForName(name);
                    if (uuid == null) {
                        throw new UnknownPlayerException("Unknown player: " + name);
                    }
                    playerTable.updateBalance(uuid, balance);
                } catch (SQLException | UnknownPlayerException e) {
                    logger.warning(e, "Failed to update balance.");
                }
            }
        });
    }

}
