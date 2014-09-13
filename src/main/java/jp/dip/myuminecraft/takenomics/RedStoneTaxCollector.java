package jp.dip.myuminecraft.takenomics;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

public class RedStoneTaxCollector implements Listener {

     class RedStoneTaxRecord extends TaxRecord {       
        long   switching;
        double rate;
        double arrears;
        double paid;

        RedStoneTaxRecord(long timestamp, OfflinePlayer player, long switching,
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
    
    class PlayerInfo {
        OfflinePlayer player;
        long          switching;
        double        arrears;

        PlayerInfo(OfflinePlayer player) {
            this.player = player;
            switching = 0;
            arrears = 0.0;
        }
    }

    JavaPlugin                     plugin;
    Logger                         logger;
    Messages                       messages;
    TaxLogger                      taxLogger;
    PeriodicCollector              collector;
    TaxTable                       table             = new TaxTable();
    Economy                        economy;
    WorldGuardPlugin               worldGuard;
    Map<OfflinePlayer, PlayerInfo> playerLookupTable = new HashMap<OfflinePlayer, PlayerInfo>();
    Set<String>                    taxFreeRegions    = new HashSet<String>();
    boolean                        enable            = false;
    boolean                        debug             = false;

    public RedStoneTaxCollector(JavaPlugin plugin, Logger logger, Messages messages,
            TaxLogger taxLogger, Economy economy, WorldGuardPlugin worldGuard)
            throws Exception {
        this.plugin = plugin;
        this.logger = logger;
        this.messages = messages;
        this.taxLogger = taxLogger;
        this.economy = economy;
        this.worldGuard = worldGuard;

        String prefix = "redstoneTax";
        loadConfig(prefix);

        try {
            collector = new PeriodicCollector(plugin, prefix) {
                protected boolean collect(OfflinePlayer payer) {
                    return collectRedstoneTax(payer);
                }
            };
        } catch (Exception e) {
            logger.warning("%s  Disabled redstone tax.", e.getMessage());
            enable = false;
        }

        if (! enable) {
            return;
        }

        collector.start();
        plugin.getServer().getPluginManager().registerEvents(this, plugin);
    }

    void loadConfig(String prefix) {
        String configEnable = prefix + ".enable";
        String configDebug = prefix + ".debug";
        String configTaxFree = prefix + ".taxFree";

        FileConfiguration config = plugin.getConfig();
        
        if (! config.contains(configEnable)) {
            logger.warning("'%s' is not configured.  Disable redstone tax.", configEnable);
            enable = false;
        } else if (! config.isBoolean(configEnable)) {
            logger.warning("'%s' is not a boolean.  Disable redstone tax.", configEnable);
            enable = false;
        } else {
            enable = config.getBoolean(configEnable);
        }

        debug = false;
        if (config.contains(configDebug)) {
            if (! config.isBoolean(configDebug)) {
                logger.warning("'%s' is not a boolean.", configDebug);
            } else {
                debug = config.getBoolean(configDebug);
            }
        }

        List<String> errors = table.loadConfig(prefix, config);
        if (! errors.isEmpty()) {
            for (String msg : errors) {
                logger.warning("%s  Disable redstone tax.", msg);
            }
            enable = false;
        }

        if (config.contains(configTaxFree)) {
            if (! config.isList(configTaxFree)) {
                logger.warning("%s is not a valid region name list.  Disable redstone tax.", configTaxFree);
                enable = false;
            } else {
                taxFreeRegions.clear();
                for (String regionId : config.getStringList(configTaxFree)) {
                    if (taxFreeRegions.contains(regionId)) {
                        logger.warning("region %s appears more than once.", regionId);
                    }
                    taxFreeRegions.add(regionId);
                }
            }
        }
        
        if (! enable) {
            table.clear();
            taxFreeRegions.clear();
        }
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
            PlayerInfo record = playerLookupTable.get(owner);
            if (record == null) {
                record = new PlayerInfo(owner);
                playerLookupTable.put(owner,  record);
                collector.addPayer(owner);
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
                    loc.getWorld().getName(), loc.getBlockX(), loc.getBlockY(), loc.getBlockZ());
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
                    PlayerInfo record = playerLookupTable.get(player);
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
    
    boolean collectRedstoneTax(OfflinePlayer payer) {
        PlayerInfo info = playerLookupTable.get(payer);
        long charge = info.switching;
        double rate = table.getRate(charge);

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
            taxLogger.put(new RedStoneTaxRecord(System.currentTimeMillis(),
                    payer, charge, rate, arrears, paid));
        }
        
        if (0.0 < arrears) {
            info.switching = 0;
            info.arrears = arrears;
            return false;
        }

        playerLookupTable.remove(payer);

        return true;
    }
}