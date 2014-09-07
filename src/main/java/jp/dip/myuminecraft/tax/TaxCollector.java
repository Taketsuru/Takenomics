package jp.dip.myuminecraft.tax;

import net.milkbowl.vault.economy.Economy;
import net.milkbowl.vault.economy.EconomyResponse;

import org.bukkit.OfflinePlayer;
import org.bukkit.plugin.RegisteredServiceProvider;
import org.bukkit.plugin.java.JavaPlugin;

public class TaxCollector {

    JavaPlugin plugin;
    Messages messages;
    Economy economy;
    int currentPlayerIndex;
    OfflinePlayer[] playerList;
    long taxIntervalStart;
    long taxPeriod;
    double middleClassStart;
    double highClassStart;
    double lowClassTaxRate;
    double middleClassTaxRate;
    double highClassTaxRate;
    boolean isEnabled;
    long tickInterval = 1000 / 20;
    TaxCollectorTask scheduledTask;
    
    public TaxCollector(JavaPlugin plugin, Messages messages) throws Exception {
        plugin.getServer().getLogger().info("TaxCollector started");
        this.plugin = plugin;
        this.messages = messages;

        isEnabled = true;

        RegisteredServiceProvider<Economy> economyProvider = 
            plugin.getServer().getServicesManager()
            .getRegistration(net.milkbowl.vault.economy.Economy.class);
        
        if (economyProvider == null) {
            String msg = String.format("[%s] - Disabled due to no Vault dependency found!", plugin.getDescription().getName());
            throw new Exception(msg);
        }

        economy = economyProvider.getProvider();
        currentPlayerIndex = -1;
        
        taxPeriod = 1000 * 60 * 60; // 1 hour
        // taxPeriod = 1000 * 20; // 20 seconds
        taxIntervalStart = System.currentTimeMillis() + taxPeriod;

        middleClassStart = 10000;
        highClassStart = 1000000;

        lowClassTaxRate = 0;
        middleClassTaxRate = 0.001;
        highClassTaxRate = 0.03;

        scheduledTask = new TaxCollectorTask(this);
        scheduledTask.runTaskLater(plugin, taxPeriod / tickInterval);
    }
    
    public void disable() {
        this.isEnabled = false;
        scheduledTask.cancel();
    }

    public void collect() {
        long startTime = System.currentTimeMillis();
        long endTime = startTime + 2;
        while (isEnabled && System.currentTimeMillis() < endTime) {

            if (currentPlayerIndex < 0) {
                playerList = plugin.getServer().getOfflinePlayers();
                currentPlayerIndex = 0;
                continue;
            }

            OfflinePlayer player = playerList[currentPlayerIndex];
            double balance = economy.getBalance(player.getName());

            double rate = 1.0;
            if (balance < middleClassStart) {
                rate = lowClassTaxRate;
            } else if (balance < highClassStart) {
                rate = middleClassTaxRate;
            } else {
                rate = highClassTaxRate;
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
                
            EconomyResponse response = economy.withdrawPlayer(player.getName(), tax);
            if (response.transactionSuccess()) {
                if (0 < tax && player.isOnline()) {
                    player.getPlayer().sendMessage(String.format(messages.getString("taxCollected"), (int)tax));
                }
            } else {
                plugin.getServer().getLogger().info(response.toString());
            }

            if (playerList.length <= ++currentPlayerIndex) {
                playerList = null;
                currentPlayerIndex = -1;
                while (taxIntervalStart <= System.currentTimeMillis()) {
                    taxIntervalStart += taxPeriod;
                }
                scheduledTask = new TaxCollectorTask(this);
                scheduledTask.runTaskLater(plugin, (taxIntervalStart - System.currentTimeMillis()) / tickInterval);

                return;
            }
        }

        if (isEnabled) {
            scheduledTask = new TaxCollectorTask(this);
            scheduledTask.runTask(plugin);
        }
    }

}
