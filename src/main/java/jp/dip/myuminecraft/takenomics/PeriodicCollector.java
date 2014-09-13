package jp.dip.myuminecraft.takenomics;

import java.util.ArrayList;
import java.util.List;

import org.bukkit.OfflinePlayer;
import org.bukkit.configuration.file.FileConfiguration;

import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;

public abstract class PeriodicCollector {

    JavaPlugin          plugin;
    BukkitRunnable      runnable;
    List<OfflinePlayer> payers = new ArrayList<OfflinePlayer>();
    long                interval;
    int                 nextPayer;

    public PeriodicCollector(JavaPlugin plugin, String configPrefix) throws Exception {
        this.plugin = plugin;
        loadConfig(configPrefix);
    }

    void loadConfig(String configPrefix) throws Exception {
        String configInterval = configPrefix + ".interval";

        FileConfiguration config = plugin.getConfig();

        if (! config.contains(configInterval)) {
            throw new Exception(String.format("'%s' is not configured.", configInterval));
        }

        if (! config.isInt(configInterval)
                && ! config.isLong(configInterval)) {
            throw new Exception(String.format("'%s' is not an integer.", configInterval));
        }

        long minInterval = 10;
        long maxInterval = 60 * 60 * 24 * 365;
        long interval = config.getLong(configInterval);
        if (interval < minInterval || maxInterval < interval) {
            throw new Exception(String.format("'%s' is out of legitimate range (min: %d, max: %d).",
                    minInterval, maxInterval));
        }

        this.interval = interval;
    }

    public void start() {
        if (runnable == null) {
            scheduleNextInterval();
        }
    }

    public void stop() {
        if (runnable != null) {
            runnable.cancel();
            runnable = null;
        }
    }

    void scheduleNextInterval() {
        nextPayer = 0;
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

    void collectFromPayers() {
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

        nextPayer = 0;
        onEndOfInterval();

        scheduleNextInterval();
    }

    protected abstract boolean collect(OfflinePlayer payer);
    
    protected void onEndOfInterval() {
    }

    public void addPayer(OfflinePlayer payer) {
        payers.add(payer);
    }
}

