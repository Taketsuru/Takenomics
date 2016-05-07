package jp.dip.myuminecraft.takenomics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import net.milkbowl.vault.economy.Economy;

import org.bukkit.Bukkit;
import org.bukkit.Chunk;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.OfflinePlayer;
import org.bukkit.Server;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.block.BlockState;
import org.bukkit.block.Dropper;
import org.bukkit.block.Hopper;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockPlaceEvent;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.ItemStack;
import org.bukkit.plugin.java.JavaPlugin;

import com.sk89q.worldguard.protection.regions.ProtectedRegion;

import jp.dip.myuminecraft.takecore.Logger;
import jp.dip.myuminecraft.takecore.Messages;

public class HopperTaxCollector extends PeriodicTaxCollector
        implements Listener {

    private static class PayerInfo {
        Set<Location> hoppers;
        double        arrears;

        PayerInfo() {
            hoppers = new HashSet<Location>();
            arrears = 0.0;
        }
    }

    private static class HopperTaxRecord extends TaxRecord {
        long   count;
        double rate;
        double arrears;
        double paid;

        HopperTaxRecord(long timestamp, OfflinePlayer player, long count,
                double rate, double arrears, double paid) {
            super(timestamp, player);
            this.count = count;
            this.rate = rate;
            this.arrears = arrears;
            this.paid = paid;
        }

        protected String subclassToString() {
            return String.format("hopper %d %f %f %f", count, rate, arrears,
                    paid);
        }
    }

    Messages             messages;
    TaxLogger            taxLogger;
    TaxTable             taxTable;
    Economy              economy;
    RegionManager        regionManager;
    Map<UUID, PayerInfo> payersTable;
    Set<String>          taxFreeRegions;
    int                  investigationIndex;
    List<Chunk>          investigationList;

    public HopperTaxCollector(JavaPlugin plugin, Logger logger,
            Messages messages, TaxLogger taxLogger, Economy economy,
            RegionManager regionManager) {
        super(plugin, logger);
        this.messages = messages;
        this.taxLogger = taxLogger;
        this.economy = economy;
        this.regionManager = regionManager;
        taxTable = new TaxTable();
        payersTable = new HashMap<UUID, PayerInfo>();
        taxFreeRegions = new HashSet<String>();
        investigationList = new ArrayList<Chunk>();
        investigationIndex = -1;
    }

    public void enable() {
        super.loadConfig(logger, plugin.getConfig(), "hopperTax",
                "hopper tax");
        if (!enable) {
            return;
        }

        plugin.getServer().getPluginManager().registerEvents(this, plugin);
    }

    public void disable() {
        investigationList.clear();
        taxTable.clear();
        taxFreeRegions.clear();
    }

    @EventHandler(ignoreCancelled = true)
    public void shouldBlockPlacement(BlockPlaceEvent event) {
        Block block = event.getBlock();
        if (block.getType() != Material.HOPPER) {
            return;
        }

        Location location = block.getLocation();
        ProtectedRegion region = regionManager
                .getHighestPriorityRegion(location);
        if (region == null) {
            messages.send(event.getPlayer(), "hopperPlacementNotAllowed");
            event.setCancelled(true);
            return;
        }

        if (taxFreeRegions.contains(region.getId())) {
            return;
        }

        List<UUID> owners = regionManager.getOwners(region);
        if (owners.isEmpty()) {
            messages.send(event.getPlayer(), "hopperPlacementNotAllowed");
            event.setCancelled(true);
            return;
        }

        UUID primaryOwner = owners.get(0);
        PayerInfo info = payersTable.get(primaryOwner);
        if (info != null && 0.0 < info.arrears) {
            messages.send(event.getPlayer(), "payTaxInArrearsToPlaceHopper");
            event.setCancelled(true);
            return;
        }
    }

    protected boolean loadConfig(Logger logger, FileConfiguration config,
            String configPrefix, boolean error) {
        boolean result = super.loadConfig(logger, config, configPrefix, error);

        if (!taxTable.loadConfig(logger, config, configPrefix + ".table")) {
            result = true;
        }

        String configTaxFree = configPrefix + ".taxExempt";
        taxFreeRegions.clear();
        if (config.contains(configTaxFree)) {
            if (!config.isList(configTaxFree)) {
                logger.warning("%s is not a valid region name list.",
                        configTaxFree);
                result = true;
            } else {
                for (String regionId : config.getStringList(configTaxFree)) {
                    if (taxFreeRegions.contains(regionId)) {
                        logger.warning("region %s appears more than once.",
                                regionId);
                    }
                    taxFreeRegions.add(regionId);
                }
            }
        }

        return result;
    }

    @Override
    protected boolean prepareCollection() {
        if (investigationIndex < 0) {
            for (World world : Bukkit.getServer().getWorlds()) {
                for (Chunk chunk : world.getLoadedChunks()) {
                    investigationList.add(chunk);
                }
            }

            if (investigationList.isEmpty()) {
                return true;
            }

            investigationIndex = 0;
        }

        long startTime = System.nanoTime();
        long maxCollectionTime = 10 * 1000 * 1000; // 10ms
        while (investigationIndex < investigationList.size()) {
            if (startTime + maxCollectionTime <= System.nanoTime()) {
                return false;
            }

            Chunk chunk = investigationList.get(investigationIndex);
            ++investigationIndex;
            if (!chunk.isLoaded()) {
                continue;
            }

            for (BlockState state : chunk.getTileEntities()) {
                if (!(state instanceof Hopper)) {
                    continue;
                }

                Location location = state.getLocation();

                ProtectedRegion region = regionManager
                        .getHighestPriorityRegion(location);
                if (region == null) {
                    continue;
                }

                if (taxFreeRegions.contains(region.getId())) {
                    continue;
                }

                List<UUID> owners = regionManager.getOwners(region);
                if (owners.isEmpty()) {
                    continue;
                }

                UUID primaryOwner = owners.get(0);
                PayerInfo info = payersTable.get(primaryOwner);
                if (info == null) {
                    info = new PayerInfo();
                    payersTable.put(primaryOwner, info);
                }

                info.hoppers.add(location);
            }
        }

        Server server = plugin.getServer();
        for (Map.Entry<UUID, PayerInfo> entry : payersTable.entrySet()) {
            if (!entry.getValue().hoppers.isEmpty()) {
                addPayer(server.getOfflinePlayer(entry.getKey()));
            }
        }

        investigationList.clear();
        investigationIndex = -1;

        return true;
    }

    @Override
    protected boolean collect(OfflinePlayer payer) {
        UUID payerId = payer.getUniqueId();
        PayerInfo info = payersTable.get(payerId);
        long charge = info.hoppers.size();
        double rate = taxTable.getRate(charge);
        double arrears = info.arrears;
        double tax = Math.floor(charge * rate + arrears);
        double balance = economy.getBalance(payer);
        long penalty = 0;
        while (balance < tax && penalty <= charge) {
            ++penalty;
            rate = taxTable.getRate(charge - penalty);
            tax = Math.floor((charge - penalty) * rate + arrears);
        }

        logger.info(
                "hopper tax: player %s, count %d, rate %g, "
                        + "arrears %g, tax %g, balance %g, penalty %d",
                payer.getName(), charge, rate, arrears, tax, balance, penalty);

        double paid = 0.0;
        if (balance <= 0.0) {
            paid = 0.0;
            arrears = tax;
        } else if (balance < tax) {
            paid = balance;
            arrears = tax - paid;
        } else {
            paid = tax;
            arrears = 0.0;
        }

        Player onlinePlayer = payer.isOnline() ? payer.getPlayer() : null;
        if (onlinePlayer != null && (0.0 < tax || 0 < penalty)) {
            messages.send(onlinePlayer, "hopperTaxNoticeHeader");
            messages.send(onlinePlayer, "hopperTaxNoticeCount", charge);
            if (0 < penalty) {
                messages.send(onlinePlayer, "hopperTaxNoticePenalty", penalty);
            }
            messages.send(onlinePlayer, "hopperTaxNoticeRate", rate);
            if (0.0 < arrears) {
                messages.send(onlinePlayer, "hopperTaxNoticeArrears", arrears);
            }
            messages.send(onlinePlayer, "hopperTaxNoticeTotal", tax);
        }

        if (0.0 < paid) {
            economy.withdrawPlayer(payer, paid);
            if (onlinePlayer != null) {
                messages.send(onlinePlayer, "hopperTaxCollected", paid);
            }
        }

        if (0.0 < paid || 0.0 < arrears) {
            taxLogger.put(new HopperTaxRecord(System.currentTimeMillis(),
                    payer, charge, rate, arrears, paid));
        }

        int messageCount = 0;
        int removeCount = 0;
        for (Location location : info.hoppers) {
            if (penalty <= removeCount) {
                break;
            }
            if (messageCount < 8 && onlinePlayer != null) {
                messages.send(onlinePlayer, "seisuredHopperLocation",
                        location.getWorld().getName(), location.getBlockX(),
                        location.getBlockY(), location.getBlockZ());
                ++messageCount;
            }
            logger.info("seisured hopper @ %s:%d,%d,%d",
                    location.getWorld().getName(), location.getBlockX(),
                    location.getBlockY(), location.getBlockZ());
            removeHopper(location);
            ++removeCount;
        }
        
        if (messageCount < removeCount && onlinePlayer != null) {
            messages.send(onlinePlayer, "moreSeisuredHoppers", removeCount - messageCount);
        }

        info.hoppers.clear();
        info.arrears = arrears;

        if (arrears == 0.0) {
            payersTable.remove(payerId);
        }

        return arrears == 0.0;
    }

    void removeHopper(Location location) {
        Block block = location.getBlock();
        if (block.getType() != Material.HOPPER) {
            return;
        }

        BlockState state = block.getState();
        Inventory oldInventory = ((Hopper) state).getInventory();
        ItemStack[] oldContents = oldInventory.getStorageContents();
        int hopperInventorySize = oldContents.length;
        ItemStack[] newContents = new ItemStack[hopperInventorySize];
        for (int i = 0; i < hopperInventorySize; ++i) {
            ItemStack itemStack = oldContents[i];
            if (itemStack != null) {
                newContents[i] = itemStack;
                oldInventory.clear(i);
            }
        }
        state.update();

        // Workaround
        // https://hub.spigotmc.org/jira/si/jira.issueviews:issue-html/SPIGOT-611/SPIGOT-611.html
        state.setType(Material.AIR);
        state.update(true);

        state = location.getBlock().getState();
        state.setType(Material.DROPPER);
        state.update(true);

        state = location.getBlock().getState();
        Inventory newInventory = ((Dropper) state).getInventory();
        for (ItemStack stack : newContents) {
            if (stack != null) {
                newInventory.addItem(stack);
            }
        }
        state.update();
    }
}