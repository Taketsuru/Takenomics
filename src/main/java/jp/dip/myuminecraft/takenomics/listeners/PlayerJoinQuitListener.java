package jp.dip.myuminecraft.takenomics.listeners;

import java.sql.SQLException;

import jp.dip.myuminecraft.takenomics.models.AccessLog;
import jp.dip.myuminecraft.takenomics.models.PlayerTable;

import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.AsyncPlayerPreLoginEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;

public class PlayerJoinQuitListener implements Listener {

    PlayerTable playerTable;
    AccessLog accessLog;
    
    public PlayerJoinQuitListener(PlayerTable playerTable, AccessLog accessLog) {
        this.playerTable = playerTable;
        this.accessLog = accessLog;
    }
    
    @EventHandler
    public void onAsyncPlayerPreLogin(AsyncPlayerPreLoginEvent event) {
        try {
            // To prevent slow down of synchronous code, we prefetch the id from DB here.
            playerTable.getId(event.getName());
        } catch (SQLException e) {
        }
    }

    @EventHandler
    public void onPlayerJoin(PlayerJoinEvent event) {
        accessLog.put(event.getPlayer(), AccessLog.EntryType.JOIN);
    }

    @EventHandler
    public void onPlayerQuit(PlayerQuitEvent event) {
        Player player = event.getPlayer();
        accessLog.put(player, AccessLog.EntryType.QUIT);
        playerTable.mayDropCache(player);
    }
}
