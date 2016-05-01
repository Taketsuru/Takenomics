package jp.dip.myuminecraft.takenomics.listeners;

import jp.dip.myuminecraft.takecore.Logger;
import jp.dip.myuminecraft.takenomics.models.AccessLog;

import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;

public class PlayerJoinQuitListener implements Listener {

    Logger    logger;
    AccessLog accessLog;

    public PlayerJoinQuitListener(Logger logger, AccessLog accessLog) {
        this.logger = logger;
        this.accessLog = accessLog;
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
