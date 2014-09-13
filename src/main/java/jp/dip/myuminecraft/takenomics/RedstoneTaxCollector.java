package jp.dip.myuminecraft.takenomics;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.milkbowl.vault.economy.Economy;

import org.bukkit.Location;
import org.bukkit.OfflinePlayer;
import org.bukkit.Server;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockPhysicsEvent;
import org.bukkit.event.block.BlockRedstoneEvent;
import org.bukkit.plugin.java.JavaPlugin;

import com.sk89q.worldguard.bukkit.WorldGuardPlugin;
import com.sk89q.worldguard.protection.regions.ProtectedRegion;

public class RedstoneTaxCollector extends PeriodicTaxCollector implements Listener {

    class RedstoneTaxRecord extends TaxRecord {       
        long   switching;
        double rate;
        double arrears;
        double paid;

        RedstoneTaxRecord(long timestamp, OfflinePlayer player, long switching,
                double rate, double arrears, double paid) {
             super(timestamp, player);
             this.switching = switching;
             this.rate = rate;
             this.arrears = arrears;
             this.paid = paid;
        }
        
        protected String subclassToString() {
            return String.format("redstone %d %f %f %f", switching, rate, arrears, paid);
        }
    }

    class PayerInfo {
        long   switching;
        double arrears;

        PayerInfo() {
            switching = 0;
            arrears = 0.0;
        }
    }

    Messages                      messages;
    TaxLogger                     taxLogger;
    TaxTable                      taxTable       = new TaxTable();
    Economy                       economy;
    WorldGuardPlugin              worldGuard;
    Map<OfflinePlayer, PayerInfo> payersTable    = new HashMap<OfflinePlayer, PayerInfo>();
    Set<String>                   taxFreeRegions = new HashSet<String>();

    public RedstoneTaxCollector(JavaPlugin plugin, Logger logger, Messages messages,
            TaxLogger taxLogger, Economy economy, WorldGuardPlugin worldGuard) {
        super(plugin, logger);
        this.messages = messages;
        this.taxLogger = taxLogger;
        this.economy = economy;
        this.worldGuard = worldGuard;
    }
    
    public void enable() {
        super.loadConfig(logger, plugin.getConfig(), "redstoneTax", "redstone tax");

        if (enable) {
            plugin.getServer().getPluginManager().registerEvents(this, plugin);
        } else {
            taxTable.clear();
            taxFreeRegions.clear();
        }
    }

    protected boolean loadConfig(Logger logger, FileConfiguration config, String configPrefix, boolean error) {
        boolean result = super.loadConfig(logger,  config, configPrefix, error);
        
        if (! taxTable.loadConfig(logger, config, configPrefix)) {
            result = true;
        }

        String configTaxFree = configPrefix + ".taxFree";
        taxFreeRegions.clear();
        if (config.contains(configTaxFree)) {
            if (! config.isList(configTaxFree)) {
                logger.warning("%s is not a valid region name list.", configTaxFree);
                result = true;
            } else {
                for (String regionId : config.getStringList(configTaxFree)) {
                    if (taxFreeRegions.contains(regionId)) {
                        logger.warning("region %s appears more than once.", regionId);
                    }
                    taxFreeRegions.add(regionId);
                }
            }
        }
        
        return result;
    }
    
    @EventHandler(priority = EventPriority.MONITOR)
    public void monitorRedstone(BlockRedstoneEvent event) {
        if ((0 < event.getOldCurrent()) == (0 < event.getNewCurrent())) {
            return;
        }

        Location loc = event.getBlock().getLocation();

        if (debug) {
            logger.info("redstone: monitor %s:%d,%d,%d",
                    loc.getWorld().getName(), loc.getBlockX(), loc.getBlockY(), loc.getBlockZ());
        }

        GetBlockOwnersResult gboResult = getBlockOwners(loc);
        if (gboResult == null) {
            if (debug) {
                logger.info("redstone: no owner");
            }
            return;
        }
        
        if (gboResult.taxFree && debug) {
            logger.info("redstone: tax free");
            return;
        }

        Server server = plugin.getServer();
        for (String ownerName : gboResult.owners) {
            OfflinePlayer owner = server.getOfflinePlayer(ownerName);
            PayerInfo record = payersTable.get(owner);
            if (record == null) {
                record = new PayerInfo();
                payersTable.put(owner,  record);
                addPayer(owner);
            }

            if (record.arrears == 0.0) {
                if (debug) {
                    logger.info("monitor: owner: %s", ownerName);
                }
                ++record.switching;
                break;
            }
        }
    }
    
    class GetBlockOwnersResult {
        boolean     taxFree;
        Set<String> owners;
        
        GetBlockOwnersResult(Set<String> owners, boolean taxFree) {
            this.owners = owners;
            this.taxFree = taxFree;
        }
    }

    GetBlockOwnersResult getBlockOwners(Location loc) {
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
        
        if (debug) {
            logger.info("redstone: hit %s",
                    highest == null ? "<none>" : highest.getId());
        }

        return highest == null
                ? null
                : new GetBlockOwnersResult(highest.getOwners().getPlayers(),
                        taxFreeRegions.contains(highest.getId()));
    }

    @EventHandler
    public void onBlockPhysicsEvent(BlockPhysicsEvent event) {
        if (event.isCancelled()) {
            return;
        }

        switch (event.getBlock().getType()) {

        case DIODE_BLOCK_ON:
        case DIODE_BLOCK_OFF:
        case REDSTONE_WIRE:
            cancelIfThereIsArrears(event);
            return;
            
        default:
            return;
        }
    }
    
    void cancelIfThereIsArrears(BlockPhysicsEvent event) {
        Location loc = event.getBlock().getLocation();

        if (debug) {
            logger.info("redstone: physics %s:%d,%d,%d",
                    loc.getWorld().getName(),
                    loc.getBlockX(), loc.getBlockY(), loc.getBlockZ());
        }

        boolean paid = false;

        GetBlockOwnersResult gboResult = getBlockOwners(loc);

        if (gboResult != null) {
            if (gboResult.taxFree) {
                paid = true;
            } else {
                Server server = plugin.getServer();
                for (String owner : gboResult.owners) {
                    OfflinePlayer player = server.getOfflinePlayer(owner);
                    PayerInfo record = payersTable.get(player);
                    if (record == null || record.arrears == 0.0) {
                        if (debug) {
                            logger.info("redstone: owner: %s", owner);
                        }
                        paid = true;
                        break;
                    }            
                }
            }
        }

        if (! paid) {
            if (debug) {
                logger.info("redstone: cancelled");
            }

            event.setCancelled(true);
        }
    }
    
    protected boolean collect(OfflinePlayer payer) {
        PayerInfo info = payersTable.get(payer);
        long charge = info.switching;
        double rate = taxTable.getRate(charge);

        double arrears = info.arrears;
        double tax = charge * rate + arrears;

        Player onlinePlayer = payer.isOnline() ? payer.getPlayer() : null;
        if (onlinePlayer != null && 0.0 < tax) {
            messages.chat(onlinePlayer, "redstoneTaxNoticeHeader");
            messages.chat(onlinePlayer, "redstoneTaxNoticeSwitching", charge);
            messages.chat(onlinePlayer, "redstoneTaxNoticeRate", rate);
            if (0.0 < arrears) {
                messages.chat(onlinePlayer, "redstoneTaxNoticeArrears", arrears);
            }
            messages.chat(onlinePlayer, "redstoneTaxNoticeTotal", tax);
        }

        String playerName = payer.getName();
        double balance = economy.getBalance(playerName);
        double paid = 0.0;
        if (balance <= 0.0) {
            paid = 0.0;
            arrears = tax;              
        } else if (balance < tax) {
            paid = balance;
            arrears = tax - balance;
        } else {
            paid = tax;
            arrears = 0.0;
        }

        if (0.0 < paid) {
            economy.withdrawPlayer(playerName, paid);
            if (onlinePlayer != null) {
                messages.chat(onlinePlayer, "redstoneTaxCollected", paid);
            }
        }

        if (0.0 < paid || 0.0 < arrears) {
            taxLogger.put(new RedstoneTaxRecord(System.currentTimeMillis(),
                    payer, charge, rate, arrears, paid));
        }
        
        if (0.0 < arrears) {
            info.switching = 0;
            info.arrears = arrears;
            return false;
        }

        payersTable.remove(payer);

        return true;
    }
}