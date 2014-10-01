package jp.dip.myuminecraft.takenomics;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.bukkit.OfflinePlayer;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;

public abstract class PeriodicTaxCollector extends TaxCollector {

    enum State {
        preparing,
        collecting,
    }

    BukkitRunnable      runnable;
    State               state;
    List<OfflinePlayer> payers = new ArrayList<OfflinePlayer>();
    long                interval;
    int                 nextPayer;

    public PeriodicTaxCollector(JavaPlugin plugin, Logger logger) {
        super(plugin, logger);
    }

    protected void loadConfig(Logger logger, FileConfiguration config, String configPrefix, String taxName) {
        super.loadConfig(logger, config, configPrefix, taxName);

        if (enable) {
            scheduleNextInterval();
        }       
    }
    
    protected boolean
    loadConfig(Logger logger, FileConfiguration config,
            String configPrefix, boolean error) {
        boolean result = super.loadConfig(logger, config, configPrefix, error);

        String configInterval = configPrefix + ".interval";
        if (! config.contains(configInterval)) {
            logger.warning("'%s' is not configured.", configInterval);
            result = true;
        } else if (! config.isInt(configInterval)
                && ! config.isLong(configInterval)) {
            logger.warning("'%s' is not an integer.", configInterval);
            result = true;
        } else {
            long minInterval = 10;
            long maxInterval = 60 * 60 * 24 * 365;
            long interval = config.getLong(configInterval);
            if (interval < minInterval || maxInterval < interval) {
                logger.warning("'%s' is out of legitimate range.",
                        configInterval);
                result = true;
            } else {
                this.interval = interval;
            }
        }
        
        return result;
    }

    public boolean loadTaxExemptList(Logger logger, FileConfiguration config,
            String configName, Set<String> taxExempt) {
        taxExempt.clear();

        if (! config.contains(configName)) {
            return false;
        }
        
        if (! config.isList(configName)) {
            logger.warning("%s is not a valid region name list.", configName);
            return true;
        }
        
        for (String regionId : config.getStringList(configName)) {
            if (taxExempt.contains(regionId)) {
                logger.warning("region %s appears more than once.", regionId);
            }
            taxExempt.add(regionId);
        }
        
        return false;
    }
    
    public void disable() {
        if (runnable != null) {
            runnable.cancel();
            runnable = null;
        }
    }

    void scheduleNextInterval() {
        state = State.preparing;
        runnable = newCollectorTask();
        runnable.runTaskLater(plugin, interval * Constants.ticksPerSecond);        
    }

    BukkitRunnable newCollectorTask() {
        return new BukkitRunnable() {
            public void run() {
                collectFromPayers();
            }
        };
    }

    protected boolean prepareCollection() {
        return true;
    }

    void collectFromPayers() {
        switch (state) {
        case preparing:
            if (! prepareCollection()) {
                runnable = newCollectorTask();
                runnable.runTask(plugin);
                return;
            }

            state = State.collecting;
            nextPayer = 0;
            // fall through
            
        case collecting:  
            long startTime = System.nanoTime();
            long maxCollectionTime = 2 * 1000 * 1000; // 2ms
            for (; nextPayer < payers.size(); ++nextPayer) {
    
                if (maxCollectionTime <= System.nanoTime() - startTime) {
                    runnable = newCollectorTask();
                    runnable.runTask(plugin);
                    return;
                }
    
                OfflinePlayer payer = payers.get(nextPayer);
                if (collect(payer)) {
                    int size = payers.size();
                    if (nextPayer < size - 1) {
                        payers.set(nextPayer, payers.get(size - 1));
                    }
                    payers.remove(size - 1);
                    --nextPayer;
                }
            }
            break;
            
        default:
            break;
        }

        scheduleNextInterval();
    }

    protected abstract boolean collect(OfflinePlayer payer);
    
    public void addPayer(OfflinePlayer payer) {
        if (! payers.contains(payer)) {
            payers.add(payer);
        }
    }
}
