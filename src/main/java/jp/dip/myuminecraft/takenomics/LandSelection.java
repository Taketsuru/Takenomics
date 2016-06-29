package jp.dip.myuminecraft.takenomics;

import org.bukkit.event.player.PlayerInteractEvent;
import org.bukkit.event.player.PlayerMoveEvent;

import com.sk89q.worldguard.protection.regions.ProtectedRegion;

import jp.dip.myuminecraft.takecore.BlockCoordinates;

public interface LandSelection {
    public void clear();

    public void onPlayerInteract(PlayerInteractEvent event);

    public void onPlayerMove(PlayerMoveEvent event);
    
    public BlockProximity getBlockProximity(BlockCoordinates coords);
    
    public boolean isSelected();
    
    public ProtectedRegion getEnclosingRegion();
    
    public ProtectedRegion createRegionFromSelection(String id);

    public int getSelectedVolume();
}
