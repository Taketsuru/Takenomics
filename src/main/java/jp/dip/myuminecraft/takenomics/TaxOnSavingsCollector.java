package jp.dip.myuminecraft.takenomics;

import java.util.List;

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
    TaxTable           table = new TaxTable();
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
                            || (table.getTaxExemptionLimit() <= economy.getBalance(player.getName()))) {
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

        if (! table.loadConfig(logger, config, configPrefix)) {
            logger.warning("Disable tax on savings.");
            enable = false;
        }
    }

    public void disable() {
        enable = false;
        collector.stop();
    }

    boolean collectTaxOnSavings(OfflinePlayer payer) {
        double balance = economy.getBalance(payer.getName());
        double rate = table.getRate(balance);

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
