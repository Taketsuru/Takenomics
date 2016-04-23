package jp.dip.myuminecraft.takenomics;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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

import com.sk89q.worldguard.protection.regions.ProtectedRegion;

import jp.dip.myuminecraft.takecore.Logger;
import jp.dip.myuminecraft.takecore.Messages;

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
    RegionManager                 regionManager;
    Map<OfflinePlayer, PayerInfo> payersTable    = new HashMap<OfflinePlayer, PayerInfo>();
    Set<String>                   taxFreeRegions = new HashSet<String>();

    public RedstoneTaxCollector(JavaPlugin plugin, Logger logger, Messages messages,
            TaxLogger taxLogger, Economy economy, RegionManager regionManager) {
        super(plugin, logger);
        this.messages = messages;
        this.taxLogger = taxLogger;
        this.economy = economy;
        this.regionManager = regionManager;
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
        
        if (! taxTable.loadConfig(logger, config, configPrefix + ".table")) {
            result = true;
        }

        String configTaxFree = configPrefix + ".taxExempt";
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
        ProtectedRegion region = regionManager.getHighestPriorityRegion(loc);
        if (region == null || taxFreeRegions.contains(region.getId())) {
            return;
        }

        Collection<UUID> owners = regionManager.getOwners(region);       
        Server server = plugin.getServer();
        for (UUID ownerUUID : owners) {
            OfflinePlayer owner = server.getOfflinePlayer(ownerUUID);
            PayerInfo record = payersTable.get(owner);
            if (record == null) {
                record = new PayerInfo();
                payersTable.put(owner,  record);
                addPayer(owner);
            }

            if (record.arrears == 0.0) {
                ++record.switching;
                break;
            }
        }
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

        ProtectedRegion region = regionManager.getHighestPriorityRegion(loc);
        boolean paid = false;
        if (region != null) {
            if (taxFreeRegions.contains(region.getId())) {
                paid = true;
            } else {  
                Server server = plugin.getServer();
                for (UUID uuid : regionManager.getOwners(region)) {
                    OfflinePlayer player = server.getOfflinePlayer(uuid);
                    PayerInfo record = payersTable.get(player);
                    if (record == null || record.arrears == 0.0) {
                        paid = true;
                        break;
                    }            
                }
            }
        }

        if (! paid) {
            event.setCancelled(true);
        }
    }
    
    protected boolean collect(OfflinePlayer payer) {
        PayerInfo info = payersTable.get(payer);
        long charge = info.switching;
        double rate = taxTable.getRate(charge);

        double arrears = info.arrears;
        double tax = Math.floor(charge * rate + arrears);

        Player onlinePlayer = payer.isOnline() ? payer.getPlayer() : null;
        if (onlinePlayer != null && 0.0 < tax) {
            messages.send(onlinePlayer, "redstoneTaxNoticeHeader");
            messages.send(onlinePlayer, "redstoneTaxNoticeSwitching", charge);
            messages.send(onlinePlayer, "redstoneTaxNoticeRate", rate);
            if (0.0 < arrears) {
                messages.send(onlinePlayer, "redstoneTaxNoticeArrears", arrears);
            }
            messages.send(onlinePlayer, "redstoneTaxNoticeTotal", tax);
        }

        double balance = economy.getBalance(payer);
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

        if (0.0 < paid) {
            economy.withdrawPlayer(payer, paid);
            if (onlinePlayer != null) {
                messages.send(onlinePlayer, "redstoneTaxCollected", paid);
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