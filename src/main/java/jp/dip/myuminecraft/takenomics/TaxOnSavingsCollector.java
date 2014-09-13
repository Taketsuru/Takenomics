package jp.dip.myuminecraft.takenomics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.milkbowl.vault.economy.Economy;
import net.milkbowl.vault.economy.EconomyResponse;

import org.bukkit.OfflinePlayer;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;

public class TaxOnSavingsCollector implements Listener {

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

    JavaPlugin         plugin;
    Logger             logger;
    Messages           messages;
    TaxLogger          taxLogger;
    Economy            economy;
    PeriodicCollector  collector;
    List<TaxClass>     classes = new ArrayList<TaxClass>();
    boolean            enable;

    public TaxOnSavingsCollector(final JavaPlugin plugin, Logger logger, Messages messages,
            TaxLogger taxLogger, final Economy economy)
            throws Exception {
        this.plugin = plugin;
        this.logger = logger;
        this.messages = messages;
        this.taxLogger = taxLogger;
        this.economy = economy;

        String configPrefix = "taxOnSavings";
        
        loadConfig(configPrefix);

        try {
            collector = new PeriodicCollector(plugin, configPrefix) {
                protected boolean collect(OfflinePlayer payer) {
                    return collectTaxOnSavings(payer);
                }
            };
        } catch (Exception e) {
            logger.warning("%s  Disable tax on savings.", e.getMessage());
            enable = false;
        }

        if (! enable) {
            return;
        }

        new BukkitRunnable() {
            public void run() {
                for (OfflinePlayer player : plugin.getServer().getOfflinePlayers()) {
                    if (player.isOnline()
                            || (0 < classes.size()
                                    && classes.get(0).min <= economy.getBalance(player.getName()))) {
                        collector.addPayer(player);
                    }
                }                
            }
        }.runTask(plugin);

        collector.start();
        plugin.getServer().getPluginManager().registerEvents(this, plugin);
    }

    void loadConfig(String configPrefix) throws Exception {

        FileConfiguration config = plugin.getConfig();

        enable = true;

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> classConfig = (List<Map<String, Object>>) config
                .getList(configPrefix + ".classes");
        if (classConfig == null) {
            String msg = String.format(
                    "Can't find class configurations.");
            throw new Exception(msg);
        }

        classes.clear();
        int index = 0;
        for (Map<String, Object> entry : classConfig) {
            String[] doubleFields = { "min", "rate" };
            double values[] = new double[doubleFields.length];

            int fieldIx = 0;
            for (String field : doubleFields) {
                Object value = entry.get(field);

                if (value == null) {
                    String msg = String
                            .format("%d-th class doesn't have '%s' field.",
                                    index + 1, field);
                    throw new Exception(msg);
                }

                if (! (value instanceof Number)) {
                    String msg = String
                            .format("'%s' field of %d-th class has an invalid value.",
                                    field, index + 1);
                    throw new Exception(msg);
                }

                values[fieldIx] = ((Number) value).doubleValue();
                ++fieldIx;
            }

            classes.add(new TaxClass(values[0], values[1]));
            ++index;
        }
    }

    public void disable() {
        enable = false;
        collector.stop();
    }

    boolean collectTaxOnSavings(OfflinePlayer payer) {
        double balance = economy.getBalance(payer.getName());

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
            EconomyResponse response = economy.withdrawPlayer(payer.getName(), tax);
            if (response.transactionSuccess()) {
                if (payer.isOnline()) {
                    messages.chat(payer.getPlayer(), "taxCollected", (long) tax);
                }

                taxLogger.put(new Record(System.currentTimeMillis(), payer, balance, rate));
            } else {
                logger.info(response.toString());
            }
        }

        return ! payer.isOnline() && rate == 0.0;
    }
    
    @EventHandler
    void onPlayerJoin(PlayerJoinEvent event) {
        collector.addPayer(plugin.getServer().getOfflinePlayer(event.getPlayer().getName()));
    }

}
