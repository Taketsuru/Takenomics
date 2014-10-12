package jp.dip.myuminecraft.takenomics;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import jp.dip.myuminecraft.takenomics.models.PlayerTable;

import org.bukkit.Location;
import org.bukkit.plugin.java.JavaPlugin;

import com.sk89q.worldguard.bukkit.WorldGuardPlugin;
import com.sk89q.worldguard.protection.regions.ProtectedRegion;

public class RegionManager {

    JavaPlugin       plugin;
    Logger           logger;
    PlayerTable      playerTable;
    WorldGuardPlugin worldGuard;

    public RegionManager(JavaPlugin plugin, Logger logger,
            PlayerTable playerTable, WorldGuardPlugin worldGuard) {
        this.plugin = plugin;
        this.logger = logger;
        this.playerTable = playerTable;
        this.worldGuard = worldGuard;
    }

    public List<UUID> getOwners(ProtectedRegion region) {
        List<UUID> result = new ArrayList<UUID>();
        //List<UUID> result = new ArrayList<UUID>(highest.getOwners().getUniqueIds());

        for (String ownerName : region.getOwners().getPlayers()) {
            UUID uuid = playerTable.getUniqueIdForName(ownerName);
            if (uuid == null) {
                logger.warning("RegionManager: unknown player: %s", ownerName);
            } else {
                result.add(uuid);
            }
        }

        return result;
    }

    public ProtectedRegion getHighestPriorityRegion(Location loc) {
        ProtectedRegion highest = null;
        int currentPriority = Integer.MIN_VALUE;
        for (ProtectedRegion region
                : worldGuard.getRegionManager(loc.getWorld()).getApplicableRegions(loc)) {
            int regionPriority = region.getPriority();
            if (currentPriority < regionPriority) {
                highest = region;
                currentPriority = regionPriority;
            }
        }

        return highest;
    }

}
