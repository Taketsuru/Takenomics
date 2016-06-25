package jp.dip.myuminecraft.takenomics;

import org.bukkit.Location;

import com.sk89q.worldguard.bukkit.WorldGuardPlugin;
import com.sk89q.worldguard.protection.ApplicableRegionSet;
import com.sk89q.worldguard.protection.managers.RegionManager;
import com.sk89q.worldguard.protection.regions.ProtectedRegion;

public class RegionUtil {

    public static ProtectedRegion getHighestPriorityRegion(Location loc) {
        WorldGuardPlugin worldGuard = WorldGuardPlugin.inst();
        RegionManager wgRegionManager = worldGuard
                .getRegionManager(loc.getWorld());
        ApplicableRegionSet regions = wgRegionManager
                .getApplicableRegions(loc);

        if (regions == null) {
            return null;
        }

        ProtectedRegion foundRegion = null;
        int currentPriority = Integer.MIN_VALUE;
        for (ProtectedRegion region : regions) {
            if (region.getId().equals("__global__")) {
                continue;
            }

            int regionPriority = region.getPriority();
            if (regionPriority <= currentPriority) {
                continue;
            }

            foundRegion = region;
            currentPriority = regionPriority;
        }

        return foundRegion;
    }

}
