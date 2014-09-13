package jp.dip.myuminecraft.takenomics;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import net.milkbowl.vault.economy.Economy;
import net.milkbowl.vault.economy.EconomyResponse;

import org.bukkit.OfflinePlayer;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.plugin.RegisteredServiceProvider;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;

public class TaxOnSavingsCollector {

    class TaxClass {
        double min;
        double rate;

        TaxClass(double min, double rate) {
            this.min = min;
            this.rate = rate;
        }
    }
    
    class Record extends TaxRecord {       
        Record(long timestamp, OfflinePlayer player, double balance, double rate) {
             super(timestamp, player);
             this.balance = balance;
             this.rate = rate;
        }
        
        protected String subclassToString() {
            return String.format("savings %f %f", balance, rate);
        }
        
        double balance;
        double rate;
    }

    static final long maxPeriodInSec = 60 * 60 * 24 * 365;
    static final long tickInterval   = 1000 / 20;
    JavaPlugin        plugin;
    Messages          messages;
    TaxLogger         logger;
    Vector<TaxClass>  classes;
    Economy           economy;
    int               nextPayer;
    OfflinePlayer[]   playerList;
    long              taxPeriod;
    boolean           isEnabled;
    BukkitRunnable    scheduledTask;

    public TaxOnSavingsCollector(JavaPlugin plugin, Messages messages,
            TaxLogger logger, Economy economy)
            throws Exception {
        this.plugin = plugin;
        this.messages = messages;
        this.logger = logger;
        this.economy = economy;

        String pluginName = plugin.getDescription().getName();

        FileConfiguration config = plugin.getConfig();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> classConfig = (List<Map<String, Object>>) config
                .getList("classes");
        if (classConfig == null) {
            String msg = String.format(
                    "[%s] - Can't find class configurations.", pluginName);
            throw new Exception(msg);
        }

        classes = new Vector<TaxClass>();
        int index = 0;
        for (Map<String, Object> entry : classConfig) {
            String[] doubleFields = { "min", "rate" };
            double values[] = new double[doubleFields.length];

            int fieldIx = 0;
            for (String field : doubleFields) {
                Object value = entry.get(field);

                if (value == null) {
                    String msg = String
                            .format("[%s] - %d-th class doesn't have '%s' field.",
                                    pluginName, index + 1, field);
                    throw new Exception(msg);
                }

                if (! (value instanceof Number)) {
                    String msg = String
                            .format("[%s] '%s' field of %d-th class has an invalid value.",
                                    pluginName, field, index + 1);
                    throw new Exception(msg);
                }

                values[fieldIx] = ((Number) value).doubleValue();
                ++fieldIx;
            }

            classes.add(new TaxClass(values[0], values[1]));
            ++index;
        }

        taxPeriod = config.getLong("taxPeriod");
        if (taxPeriod <= 0 || maxPeriodInSec < taxPeriod) {
            String msg = String.format("[%s] taxPeriod is too large. (max: %d)",
                    pluginName, maxPeriodInSec);
            throw new Exception(msg);
        }

        isEnabled = true;

        scheduleNextCollection();
    }

    public void disable() {
        this.isEnabled = false;
        scheduledTask.cancel();
    }

    void collect() {
        long startTime = System.currentTimeMillis();
        long endTime = startTime + 2;
        for (; nextPayer < playerList.length; ++nextPayer) {
            
            long current = System.currentTimeMillis();
            if (endTime <= current) {
                scheduledTask = newCollector();
                scheduledTask.runTask(plugin);
                return;
            }

            OfflinePlayer player = playerList[nextPayer];
            double balance = economy.getBalance(player.getName());

            double rate = 0.0;
            for (TaxClass tc : classes) {
                if (balance < tc.min) {
                    break;
                }
                rate = tc.rate;
            }

            double tax;
            if (balance < 0) {
                tax = 0;
            } else {
                tax = balance * rate;
                if (balance < tax) {
                    tax = balance;
                }
            }
            tax = Math.floor(tax);

            if (0 < tax) {
                EconomyResponse response = economy.withdrawPlayer(player.getName(), tax);
                if (response.transactionSuccess()) {
                    if (player.isOnline()) {
                        player.getPlayer().sendMessage(
                                String.format(messages.getString("taxCollected"),
                                        (long) tax));
                    }

                    logger.put(new Record(current, player, balance, rate));
                } else {
                    plugin.getServer().getLogger().info(response.toString());
                }
            }
        }

        scheduleNextCollection();
    }
    
    void scheduleNextCollection() {
        playerList = plugin.getServer().getOfflinePlayers();
        nextPayer = 0;

        scheduledTask = newCollector();
        scheduledTask.runTaskLater(plugin, taxPeriod * Constants.ticksPerSecond);        
    }

    BukkitRunnable newCollector() {
        return new BukkitRunnable() {
            public void run() {
                collect();
            }
        };
    }
}
