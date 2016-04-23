package jp.dip.myuminecraft.takenomics.listeners;

import java.sql.SQLException;

import jp.dip.myuminecraft.takecore.Logger;
import jp.dip.myuminecraft.takenomics.models.AccessLog;
import jp.dip.myuminecraft.takenomics.models.PlayerTable;

import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.AsyncPlayerPreLoginEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;

public class PlayerJoinQuitListener implements Listener {

    Logger      logger;
    PlayerTable playerTable;
    AccessLog   accessLog;
    
    public PlayerJoinQuitListener(Logger logger, PlayerTable playerTable, AccessLog accessLog) {
        this.logger = logger;
        this.playerTable = playerTable;
        this.accessLog = accessLog;
    }
    
    @EventHandler
    public void onAsyncPlayerPreLogin(AsyncPlayerPreLoginEvent event) {
        if (playerTable != null) {
            try {
                playerTable.enter(event.getUniqueId(), event.getName());
            } catch (SQLException e) {
                logger.warning(e, "Failed to enter a player record.");
            }
        }
    }

    @EventHandler
    public void onPlayerJoin(PlayerJoinEvent event) {
        if (accessLog != null) {
            accessLog.put(event.getPlayer(), AccessLog.EntryType.JOIN);
        }
    }

    @EventHandler
    public void onPlayerQuit(PlayerQuitEvent event) {
        if (accessLog != null) {
            accessLog.put(event.getPlayer(), AccessLog.EntryType.QUIT);
        }
    }
}
