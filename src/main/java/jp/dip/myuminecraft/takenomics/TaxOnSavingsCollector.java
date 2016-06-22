package jp.dip.myuminecraft.takenomics;

import net.milkbowl.vault.economy.Economy;
import net.milkbowl.vault.economy.EconomyResponse;

import org.bukkit.OfflinePlayer;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;

import jp.dip.myuminecraft.takecore.Logger;
import jp.dip.myuminecraft.takecore.Messages;

public class TaxOnSavingsCollector extends PeriodicTaxCollector implements Listener {

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

    Messages  messages;
    TaxLogger taxLogger;
    Economy   economy;
    TaxTable  table = new TaxTable();

    public TaxOnSavingsCollector(JavaPlugin plugin, Logger logger, Messages messages,
            TaxLogger taxLogger, Economy economy) {
        super(plugin, logger);

        this.messages = messages;
        this.taxLogger = taxLogger;
        this.economy = economy;
    }
    
    public void enable() {
        super.loadConfig(logger, plugin.getConfig(), "taxOnSavings", "tax on savings");

        if (! enable) {
            return;
        }

        new BukkitRunnable() {
            public void run() {
                for (OfflinePlayer player : plugin.getServer().getOfflinePlayers()) {
                    if (player.isOnline()
                            || (table.getTaxExemptionLimit() <= economy.getBalance(player))) {
                        addPayer(player);
                    }
                }                
            }
        }.runTask(plugin);

        plugin.getServer().getPluginManager().registerEvents(this, plugin);
    }

    protected boolean loadConfig(Logger logger, FileConfiguration config, String configPrefix, boolean error) {
        boolean result = super.loadConfig(logger,  config, configPrefix, error);
        
        if (! table.loadConfig(logger, config, configPrefix + ".table")) {
            result = true;
        }
        
        return result;
    }

    protected boolean collect(OfflinePlayer payer) {
        double balance = economy.getBalance(payer);
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
            EconomyResponse response = economy.withdrawPlayer(payer, tax);
            if (response.transactionSuccess()) {
                if (payer.isOnline()) {
                    messages.send(payer.getPlayer(), "taxCollected", (long) tax);
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
        addPayer(event.getPlayer());
    }

}
