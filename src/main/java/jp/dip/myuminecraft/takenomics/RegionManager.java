package jp.dip.myuminecraft.takenomics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.bukkit.Location;
import org.bukkit.Server;
import org.bukkit.plugin.java.JavaPlugin;

import com.sk89q.worldguard.bukkit.WorldGuardPlugin;
import com.sk89q.worldguard.protection.regions.ProtectedRegion;

public class RegionManager {

    JavaPlugin       plugin;
    Logger           logger;
    WorldGuardPlugin worldGuard;

    public RegionManager(JavaPlugin plugin, Logger logger, WorldGuardPlugin worldGuard) {
        this.plugin = plugin;
        this.logger = logger;
        this.worldGuard = worldGuard;
    }

    @SuppressWarnings("deprecation")
    public List<UUID> getOwners(ProtectedRegion region) {
        List<UUID> result = new ArrayList<UUID>();
        //List<UUID> result = new ArrayList<UUID>(highest.getOwners().getUniqueIds());

        Server server = plugin.getServer();
        for (String ownerName : region.getOwners().getPlayers()) {
            result.add(server.getOfflinePlayer(ownerName).getUniqueId());
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
