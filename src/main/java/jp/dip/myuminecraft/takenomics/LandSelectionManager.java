package jp.dip.myuminecraft.takenomics;

import java.util.HashMap;

import org.bukkit.Bukkit;
import org.bukkit.Material;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerChangedWorldEvent;
import org.bukkit.event.player.PlayerInteractEvent;
import org.bukkit.event.player.PlayerMoveEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.ItemMeta;
import org.bukkit.plugin.java.JavaPlugin;

import com.sk89q.worldguard.protection.regions.ProtectedRegion;

import jp.dip.myuminecraft.takecore.BlockCoordinates;
import jp.dip.myuminecraft.takecore.Logger;
import jp.dip.myuminecraft.takecore.Messages;

public class LandSelectionManager implements Listener {

    private JavaPlugin                        plugin;
    private Logger                            logger;
    private Messages                          messages;
    private String                            toolName;
    private HashMap<Player, SelectionSession> sessions = new HashMap<>();

    public LandSelectionManager(JavaPlugin plugin, Logger logger,
            Messages messages) {
        this.plugin = plugin;
        this.logger = logger;
        this.messages = messages;
    }

    public void enable() throws Throwable {
        toolName = messages.getString("selectionTool");
        Bukkit.getPluginManager().registerEvents(this, plugin);
    }

    public void disable() {
        for (SelectionSession session : sessions.values()) {
            session.clear();
        }
        sessions.clear();

        toolName = null;
    }

    public BlockProximity getBlockProximity(Player player,
            BlockCoordinates coords) {
        return getSession(player).getBlockProximity(coords);
    }

    public boolean isSelected(Player player) {
        return getSession(player).isSelected();
    }

    public ProtectedRegion getEnclosingRegion(Player player) {
        return getSession(player).getEnclosingRegion();
    }

    public ProtectedRegion createRegionFromSelection(Player player,
            String id) {
        return getSession(player).createRegionFromSelection(id);
    }

    public int getSelectedVolume(Player player) {
        return getSession(player).getSelectedVolume();
    }

    @EventHandler
    public void onPlayerQuit(PlayerQuitEvent event) {
        clearSession(event.getPlayer());
    }

    @EventHandler
    public void onPlayerChangedWorld(PlayerChangedWorldEvent event) {
        clearSession(event.getPlayer());
    }

    @EventHandler
    public void onPlayerMove(PlayerMoveEvent event) {
        getSession(event.getPlayer()).onPlayerMove(event);
    }

    public void clearSession(Player player) {
        SelectionSession session = sessions.remove(player);
        if (session != null) {
            session.clear();
        }
    }

    @EventHandler(priority = EventPriority.HIGHEST)
    public void onPlayerInteract(PlayerInteractEvent event) {
        ItemStack tool = event.getItem();
        if (tool == null || tool.getType() != Material.GOLD_HOE) {
            return;
        }

        ItemMeta itemMeta = tool.getItemMeta();
        if (!itemMeta.hasDisplayName()
                || !itemMeta.getDisplayName().equals(toolName)) {
            return;
        }

        event.setCancelled(true);

        getSession(event.getPlayer()).onPlayerInteract(event);
    }

    protected SelectionSession getSession(Player player) {
        SelectionSession session = sessions.get(player);
        if (session == null) {
            session = new SelectionSession(logger, messages, player,
                    player.getWorld());
            sessions.put(player, session);
        }
        return session;
    }

}
