package jp.dip.myuminecraft.takenomics;

import java.util.HashMap;

import org.bukkit.World;
import org.bukkit.entity.Player;
import org.bukkit.event.player.PlayerInteractEvent;
import org.bukkit.event.player.PlayerMoveEvent;
import org.bukkit.inventory.meta.ItemMeta;

import com.sk89q.worldguard.protection.regions.ProtectedRegion;

import jp.dip.myuminecraft.takecore.BlockCoordinates;
import jp.dip.myuminecraft.takecore.Logger;
import jp.dip.myuminecraft.takecore.Messages;

public class SelectionSession {
    private Logger                         logger;
    private Messages                       messages;
    private LandSelection                  selection;
    private Player                         player;
    private World                          world;
    private HashMap<String, LandSelection> selections = new HashMap<>();

    SelectionSession(Logger logger, Messages messages, Player player,
            World world) {
        this.logger = logger;
        this.messages = messages;
        this.player = player;
        this.world = world;
        // selections.put(messages.getString("selectionToolExtrudedPoly"),
        // new ExtrudedPolyLandSelection(this));
        selections.put(messages.getString("selectionToolCuboid"),
                new CuboidLandSelection(logger, player));
    }

    public void clear() {
        if (selection != null) {
            selection.clear();
            selection = null;
        }
    }

    public void onPlayerInteract(PlayerInteractEvent event) {
        assert event.getPlayer().getWorld() == world;

        ItemMeta itemMeta = event.getItem().getItemMeta();
        if (!itemMeta.hasLore()) {
            messages.send(player, "selectionToolUnknownLore");
            return;
        }

        LandSelection newSelection = selections.get(itemMeta.getLore().get(0));
        if (newSelection == null) {
            messages.send(player, "selectionToolUnknownLore");
            return;
        }

        if (selection != newSelection) {
            if (selection != null) {
                selection.clear();
            }
            selection = newSelection;
        }

        selection.onPlayerInteract(event);
    }

    public boolean isSelected() {
        return selection != null && selection.isSelected();
    }

    public BlockProximity getBlockProximity(BlockCoordinates coords) {
        return selection != null ? selection.getBlockProximity(coords)
                : BlockProximity.OFF;
    }

    public ProtectedRegion getEnclosingRegion() {
        return selection != null ? selection.getEnclosingRegion() : null;
    }

    public ProtectedRegion createRegionFromSelection(String id) {
        return selection != null ? selection.createRegionFromSelection(id)
                : null;
    }

    public int getSelectedVolume() {
        return selection != null ? selection.getSelectedVolume() : 0;
    }

    public void onPlayerMove(PlayerMoveEvent event) {
        if (selection != null) {
            selection.onPlayerMove(event);
        }
    }

}
