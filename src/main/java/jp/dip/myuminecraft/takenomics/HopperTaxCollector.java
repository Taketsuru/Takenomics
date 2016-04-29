package jp.dip.myuminecraft.takenomics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import net.milkbowl.vault.economy.Economy;

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
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockBreakEvent;
import org.bukkit.event.block.BlockPlaceEvent;
import org.bukkit.event.world.ChunkLoadEvent;
import org.bukkit.event.world.ChunkUnloadEvent;
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

    Messages                   messages;
    TaxLogger                  taxLogger;
    TaxTable                   taxTable;
    Economy                    economy;
    RegionManager              regionManager;
    Map<UUID, PayerInfo>       payersTable;
    Set<String>                taxFreeRegions;
    Map<Chunk, List<Location>> hopperLocations;
    Iterator<List<Location>>   chunksWithHoppersIter;
    Iterator<Location>         hoppersInChunkIter;
    boolean                    investigating;

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
        hopperLocations = new HashMap<Chunk, List<Location>>();
        chunksWithHoppersIter = null;
        hoppersInChunkIter = null;
        investigating = false;
    }

    public void enable() {
        super.loadConfig(logger, plugin.getConfig(), "hopperTax",
                "hopper tax");

        if (enable) {
            int chunkCount = 0;
            int hopperCount = 0;
            for (World world : plugin.getServer().getWorlds()) {
                Chunk[] chunks = world.getLoadedChunks();
                chunkCount += chunks.length;
                for (int i = 0; i < chunks.length; ++i) {
                    findAllHoppersInChunk(chunks[i]);
                    if (hopperLocations.containsKey(chunks[i])) {
                        hopperCount += hopperLocations.get(chunks[i]).size();
                    }
                }
            }
            logger.info("Found %d hoppers in %d chunks.", hopperCount,
                    chunkCount);

            plugin.getServer().getPluginManager().registerEvents(this, plugin);
        } else {
            taxTable.clear();
            taxFreeRegions.clear();
            hopperLocations.clear();
        }
    }

    @EventHandler
    public void onChunkLoadEvent(ChunkLoadEvent event) {
        findAllHoppersInChunk(event.getChunk());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onChunkUnloadEvent(ChunkUnloadEvent event) {
        Chunk chunk = event.getChunk();
        if (hopperLocations.remove(chunk) != null) {
            restartInvestigation();
        }
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

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void didBlockPlacement(BlockPlaceEvent event) {
        Block block = event.getBlock();
        if (block.getType() != Material.HOPPER) {
            return;
        }

        Chunk chunk = block.getChunk();
        List<Location> list = hopperLocations.get(chunk);
        if (list == null) {
            list = new ArrayList<Location>();
            hopperLocations.put(chunk, list);
        }
        list.add(block.getLocation());

        restartInvestigation();
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void didBlockBreak(BlockBreakEvent event) {
        Block block = event.getBlock();
        if (block.getType() != Material.HOPPER) {
            return;
        }

        Chunk chunk = block.getChunk();
        List<Location> list = hopperLocations.get(chunk);
        if (list == null) {
            return;
        }

        if (list.size() == 1) {
            hopperLocations.remove(chunk);
        } else {
            list.remove(block.getLocation());
        }

        restartInvestigation();
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
        investigating = true;

        long startTime = System.nanoTime();
        long maxCollectionTime = 10 * 1000 * 1000; // 10ms
        boolean result = false;
        while (!result && System.nanoTime() - startTime < maxCollectionTime) {
            result = investigate();
        }

        if (result) {
            Server server = plugin.getServer();
            for (Map.Entry<UUID, PayerInfo> entry : payersTable.entrySet()) {
                if (!entry.getValue().hoppers.isEmpty()) {
                    addPayer(server.getOfflinePlayer(entry.getKey()));
                }
            }
            investigating = false;
        }

        return result;
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
        if (onlinePlayer != null && 0.0 < tax) {
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

        boolean showSeisuredHopper = penalty < 8;
        for (Location location : info.hoppers) {
            if (penalty == 0) {
                break;
            }
            if (showSeisuredHopper && onlinePlayer != null) {
                messages.send(onlinePlayer, "seisuredHopperLocation",
                        location.getWorld().getName(), location.getBlockX(),
                        location.getBlockY(), location.getBlockZ());
            }
            logger.info("seisured hopper @ %s:%d,%d,%d",
                    location.getWorld().getName(), location.getBlockX(),
                    location.getBlockY(), location.getBlockZ());
            removeHopper(location);
            --penalty;
        }

        info.hoppers.clear();
        info.arrears = arrears;

        if (arrears == 0.0) {
            payersTable.remove(payerId);
        }

        return arrears == 0.0;
    }

    void restartInvestigation() {
        if (!investigating) {
            return;
        }
        chunksWithHoppersIter = null;
        hoppersInChunkIter = null;
        for (PayerInfo payer : payersTable.values()) {
            payer.hoppers.clear();
        }
    }

    void findAllHoppersInChunk(Chunk chunk) {
        List<Location> hoppers = new ArrayList<Location>();
        assert !hopperLocations.containsKey(chunk);

        for (BlockState state : chunk.getTileEntities()) {
            if (state instanceof Hopper) {
                hoppers.add(state.getLocation());
            }
        }

        if (!hoppers.isEmpty()) {
            hopperLocations.put(chunk, hoppers);
            restartInvestigation();
        }
    }

    boolean investigate() {
        while (hoppersInChunkIter == null || !hoppersInChunkIter.hasNext()) {
            hoppersInChunkIter = null;

            if (chunksWithHoppersIter == null) {
                chunksWithHoppersIter = hopperLocations.values().iterator();
            }

            if (!chunksWithHoppersIter.hasNext()) {
                chunksWithHoppersIter = null;
                return true;
            }

            hoppersInChunkIter = chunksWithHoppersIter.next().iterator();
        }

        Location location = hoppersInChunkIter.next();

        ProtectedRegion region = regionManager
                .getHighestPriorityRegion(location);
        if (region == null) {
            return false;
        }

        if (taxFreeRegions.contains(region.getId())) {
            return false;
        }

        List<UUID> owners = regionManager.getOwners(region);
        if (owners.isEmpty()) {
            return false;
        }

        UUID primaryOwner = owners.get(0);
        PayerInfo info = payersTable.get(primaryOwner);
        if (info == null) {
            info = new PayerInfo();
            payersTable.put(primaryOwner, info);
        }

        info.hoppers.add(location);

        return false;
    }

    void removeHopper(Location location) {
        Block block = location.getBlock();
        hopperLocations.get(block.getChunk()).remove(location);
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