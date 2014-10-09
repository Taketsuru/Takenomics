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

import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.OfflinePlayer;
import org.bukkit.Server;
import org.bukkit.World;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.entity.Ageable;
import org.bukkit.entity.Animals;
import org.bukkit.entity.Chicken;
import org.bukkit.entity.Cow;
import org.bukkit.entity.Entity;
import org.bukkit.entity.Horse;
import org.bukkit.entity.Ocelot;
import org.bukkit.entity.Pig;
import org.bukkit.entity.Player;
import org.bukkit.entity.Sheep;
import org.bukkit.entity.Tameable;
import org.bukkit.entity.Wolf;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.entity.CreatureSpawnEvent;
import org.bukkit.event.player.PlayerEggThrowEvent;
import org.bukkit.event.player.PlayerInteractEntityEvent;
import org.bukkit.inventory.ItemStack;
import org.bukkit.plugin.java.JavaPlugin;

import com.sk89q.worldguard.protection.regions.ProtectedRegion;

public class LivestockTaxCollector extends PeriodicTaxCollector implements Listener {

    class LivestockTaxRecord extends TaxRecord {       
        int    untamed;
        int    tamed;
        double arrears;
        double paid;

        LivestockTaxRecord(long timestamp, OfflinePlayer player, int untamed, int tamed,
                double arrears, double paid) {
             super(timestamp, player);
             this.untamed = untamed;
             this.tamed = tamed;
             this.arrears = arrears;
             this.paid = paid;
        }
        
        protected String subclassToString() {
            return String.format("livestock %d %d %f %f", untamed, tamed, arrears, paid);
        }
    }

    class PayerInfo {
        int    tamedCount;
        int    untamedCount;
        double arrears = 0.0;
    }

    static final long    maxPerTick        = 2 * 1000 * 1000;               // 2ms
    Messages             messages;
    Database             database;
    TaxLogger            taxLogger;
    RegionManager        regionManager;
    Economy              economy;
    Set<String>          taxExempt         = new HashSet<String>();
    TaxTable             untamedTaxTable   = new TaxTable();
    TaxTable             tamedTaxTable     = new TaxTable();
    Map<UUID, PayerInfo> accountingRecords = new HashMap<UUID, PayerInfo>();
    Iterator<World>      worldIter;
    World                world;
    Iterator<Animals>    entityIter;

    public LivestockTaxCollector(JavaPlugin plugin, Logger logger, Messages messages,
            Database database, TaxLogger taxLogger, 
            RegionManager regionManager, Economy economy) {
        super(plugin, logger);
        this.messages = messages;
        this.database = database;
        this.taxLogger = taxLogger;
        this.regionManager = regionManager;
        this.economy = economy;
    }

    public void enable() {
        super.loadConfig(logger, plugin.getConfig(), "livestockTax", "livestock tax");

        if (! enable) {
            taxExempt.clear();
            untamedTaxTable.clear();
            tamedTaxTable.clear();
            accountingRecords.clear();
            worldIter = null;
            world = null;
            entityIter = null;

            return;
        }

        plugin.getServer().getPluginManager().registerEvents(this, plugin);
    }

    protected boolean loadConfig(Logger logger, FileConfiguration config, String configPrefix, boolean error) {
        boolean result = super.loadConfig(logger,  config, configPrefix, error);
        
        if (! untamedTaxTable.loadConfig(logger, config, configPrefix + ".untamedTable")) {
            result = true;
        }

        if (! tamedTaxTable.loadConfig(logger, config, configPrefix + ".tamedTable")) {
            result = true;
        }

        if (loadTaxExemptList(logger, config, configPrefix + ".taxExempt", taxExempt)) {
            result = true;
        }
        
        return result;
    }
    
    @Override
    protected boolean prepareCollection() {
        Server server = plugin.getServer();

        if (worldIter == null) {
            worldIter = new ArrayList<World>(plugin.getServer().getWorlds()).iterator();
            world = null;
        }

        long start = System.nanoTime();
        long end = start + maxPerTick;
        while (System.nanoTime() < end) {

            if (world == null) {
                if (! worldIter.hasNext()) {
                    worldIter = null;
                    return true;
                }

                world = worldIter.next();
                entityIter = world.getEntitiesByClass(Animals.class).iterator();
            }

            if (! entityIter.hasNext()) {
                world = null;
                entityIter = null;
                continue;
            }

            Entity entity = entityIter.next();
            OfflinePlayer payer = null;
            if (! (entity instanceof Animals)) {
                continue;
            }
                   
            boolean tamed = entity instanceof Tameable && ((Tameable)entity).isTamed();
            if (tamed) {
                UUID owner = ((Tameable)entity).getOwner().getUniqueId();
                payer = server.getOfflinePlayer(owner);
            } else {
                payer = findLivestockTaxPayer(server, entity);
            }

            if (payer == null) {
                continue;
            }
            UUID payerId = payer.getUniqueId();
            PayerInfo info = accountingRecords.get(payerId);
            if (info == null) {
                info = new PayerInfo();
                accountingRecords.put(payerId, info);
                addPayer(payer);
            }
            if (tamed) {
                ++info.tamedCount;
            } else {
                ++info.untamedCount;
            }
        }

        return false;            
   }

    OfflinePlayer findLivestockTaxPayer(Server server, Entity entity) {
        ProtectedRegion region =
                regionManager.getHighestPriorityRegion(entity.getLocation());
        if (region == null) {
            return null;
        }
        
        if (taxExempt.contains(region.getId())) {
            return null;
        }

        List<UUID> owners = regionManager.getOwners(region);
        if (owners.isEmpty()) {
            return null;
        }
        
        UUID primaryOwner = owners.get(0);
        
        return server.getOfflinePlayer(primaryOwner);
    }

    @Override
    protected boolean collect(OfflinePlayer payer) {
        PayerInfo info = accountingRecords.get(payer.getUniqueId());
        if (info == null) {
            return true;
        }

        int livestockHeadCount = info.untamedCount;
        int petHeadCount = info.tamedCount;

        double livestockRate = untamedTaxTable.getRate(livestockHeadCount);
        double petRate = tamedTaxTable.getRate(petHeadCount);
        double tax = Math.floor(livestockRate * livestockHeadCount + petRate * petHeadCount + info.arrears);
        double balance = economy.getBalance(payer);
        double paid = Math.min(tax, Math.max(0.0, Math.floor(balance)));

        if (payer.isOnline() && 0.0 < tax) {
            Player player = (Player)payer;
            messages.send(player, "mobTaxNoticeHeader");
            messages.send(player, "mobTaxNoticeLivestockCount", livestockHeadCount);
            messages.send(player, "mobTaxNoticeLivestockRate", livestockRate);
            messages.send(player, "mobTaxNoticePetCount", petHeadCount);
            messages.send(player, "mobTaxNoticePetRate", petRate);
            if (0.0 < info.arrears) {
                messages.send(player, "mobTaxNoticeArrears", info.arrears);
            }
            messages.send(player, "mobTaxNoticeTotal", tax);            
        }

        if (0.0 < paid || 0.0 < info.arrears) {
            taxLogger.put(new LivestockTaxRecord(System.currentTimeMillis(),
                    payer, livestockHeadCount, petHeadCount, info.arrears, paid));
        }

        economy.withdrawPlayer(payer, paid);

        info.untamedCount = 0;
        info.tamedCount = 0;
        info.arrears = tax - paid;
        if (tax <= paid) {
            accountingRecords.remove(payer.getUniqueId());
            return true;
        }

        return false;
    }

    @SuppressWarnings("deprecation")
    @EventHandler
    public void onPlayerInteractEntity(PlayerInteractEntityEvent event) {
        if (event.isCancelled()) {
            return;
        }

        Entity entity = event.getRightClicked();
        if (! (entity instanceof Animals)) {
            return;
        }

        Player player = event.getPlayer();
        ItemStack items = player.getItemInHand();
        if (items == null) {
            return;
        }

        if (items.getType() == Material.STICK
                && entity instanceof Tameable
                && player.equals(((Tameable)entity).getOwner())) {
            event.setCancelled(true);
            ((Tameable)entity).setOwner(null);
            return;
        }

        if (entity instanceof Ageable
                && isBreedableWith((Ageable)entity, items.getType())) {

            if (hasArrears(player)) {
                messages.send(player, "mobTaxBreedHasArrears");
            } else if (! (entity instanceof Tameable && ((Tameable)entity).isTamed())
                    && ! isInPlayersLand(player, entity.getLocation())) {
                messages.send(player, "mobTaxBreedNoRegion");
            } else {
                return;
            }

            event.setCancelled(true);
            event.getPlayer().updateInventory();
            
            return;
        }
    }       

    public boolean isBreedableWith(Ageable ageable, Material material) {
        if (! ageable.canBreed()) {
            return false;
        }
        
        if (ageable instanceof Chicken) {
            return material.equals(Material.SEEDS);
        }

        if (ageable instanceof Sheep|| ageable instanceof Cow) {
            return material.equals(Material.WHEAT);
        }
        
        if (ageable instanceof Pig) {
            return material.equals(Material.CARROT_ITEM);
        }
        
        if (ageable instanceof Wolf) {
            switch (material) {
            case COOKED_BEEF:
            case COOKED_CHICKEN:
            case RAW_BEEF:
            case RAW_CHICKEN:
            case ROTTEN_FLESH:
                return true;
            default:
                return false;
            }
        }
        
        if (ageable instanceof Ocelot) {
            return material.equals(Material.RAW_FISH);
        }

        if (ageable instanceof Horse) {
            return material.equals(Material.GOLDEN_APPLE)
                    || material.equals(Material.GOLDEN_CARROT);
        }

        return false;
    }

    @EventHandler
    public void onPlayerEggThrow(PlayerEggThrowEvent event) {
        Player player = event.getPlayer();
        if (hasArrears(player)) {
            messages.send(player, "mobTaxBreedHasArrears");
        } else if (! isInPlayersLand(player, event.getEgg().getLocation())) {
            messages.send(player, "mobTaxBreedNoRegion");
        } else {
            return;
        }
        event.setNumHatches((byte)0);
    }

    boolean isInPlayersLand(Player player, Location location) {
        UUID playerId = player.getUniqueId();
        ProtectedRegion region = regionManager.getHighestPriorityRegion(location);
        return region != null && regionManager.getOwners(region).contains(playerId);
    }

    boolean hasArrears(Player player) {
        UUID playerId = player.getUniqueId();
        return accountingRecords.containsKey(playerId)
                && 0.0 < accountingRecords.get(playerId).arrears;
    }

    @EventHandler
    public void onCreatureSpawn(CreatureSpawnEvent event) {
        if (event.isCancelled()) {
            return;
        }
        
        Entity entity = event.getEntity();
        
        switch (event.getSpawnReason()) {
        case DISPENSE_EGG:
            event.setCancelled(true);
            break;

        case NATURAL:
            break;

        default:
            return;   
        }

        if (! (entity instanceof Animals)) {
            return;
        }

        ProtectedRegion region = regionManager.getHighestPriorityRegion(event.getLocation());
        if (region != null && ! taxExempt.contains(region.getId())) {
            logger.info("%s spawn cancelled in %s @ %s:%d,%d,%d",
                    entity.getClass().getName(),
                    region.getId(),
                    event.getLocation().getWorld().getName(),
                    event.getLocation().getBlockX(),
                    event.getLocation().getBlockY(),
                    event.getLocation().getBlockZ());
            event.setCancelled(true);
        }
    }

}
