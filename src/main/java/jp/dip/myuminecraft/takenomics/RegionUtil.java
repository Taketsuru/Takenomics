package jp.dip.myuminecraft.takenomics;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.bukkit.Location;

import com.sk89q.worldguard.bukkit.WorldGuardPlugin;
import com.sk89q.worldguard.protection.regions.ProtectedRegion;

public class RegionUtil {

    public static List<UUID> getOwners(ProtectedRegion region) {
        return new ArrayList<UUID>(region.getOwners().getUniqueIds());
    }

    public static ProtectedRegion getHighestPriorityRegion(Location loc) {
        ProtectedRegion highest = null;
        int currentPriority = Integer.MIN_VALUE;
        for (ProtectedRegion region : WorldGuardPlugin.inst()
                .getRegionManager(loc.getWorld()).getApplicableRegions(loc)) {
            int regionPriority = region.getPriority();
            if (currentPriority < regionPriority) {
                highest = region;
                currentPriority = regionPriority;
            }
        }

        return highest;
    }

}
