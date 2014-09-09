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

public class TaxCollector {

    class TaxClass {
        double min;
        double rate;

        TaxClass(double min, double rate) {
            this.min = min;
            this.rate = rate;
        }
    }

    static final long maxPeriodInSec = 60 * 60 * 24 * 365;
    static final long tickInterval   = 1000 / 20;

    JavaPlugin        plugin;
    Messages          messages;
    TaxLogger         logger;
    Vector<TaxClass>  classes;
    Economy           economy;
    int               currentPlayerIndex;
    OfflinePlayer[]   playerList;
    long              taxIntervalStart;
    long              taxPeriod;
    boolean           isEnabled;
    TaxCollectorTask  scheduledTask;

    public TaxCollector(JavaPlugin plugin, Messages messages, TaxLogger logger) throws Exception {
        this.plugin = plugin;
        this.messages = messages;
        this.logger = logger;

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

        long taxPeriodInSec = config.getLong("taxPeriod");
        if (taxPeriodInSec <= 0 || maxPeriodInSec < taxPeriodInSec) {
            String msg = String.format("[%s] taxPeriod is too large. (max: %d)",
                    pluginName, maxPeriodInSec);
            throw new Exception(msg);
        }
        taxPeriod = taxPeriodInSec * 1000;

        isEnabled = true;

        RegisteredServiceProvider<Economy> economyProvider = plugin.getServer()
                .getServicesManager()
                .getRegistration(net.milkbowl.vault.economy.Economy.class);

        if (economyProvider == null) {
            String msg = String.format(
                    "[%s] Disabled due to no Vault dependency found!", pluginName);
            throw new Exception(msg);
        }

        economy = economyProvider.getProvider();
        currentPlayerIndex = -1;

        taxIntervalStart = System.currentTimeMillis() + taxPeriod;

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
        while (isEnabled) {
            
            long current = System.currentTimeMillis();
            if (endTime <= current) {
                break;
            }

            if (currentPlayerIndex < 0) {
                playerList = plugin.getServer().getOfflinePlayers();
                currentPlayerIndex = 0;
                continue;
            }

            OfflinePlayer player = playerList[currentPlayerIndex];
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

                    logger.put(current, player.getName(), balance, rate);
                } else {
                    plugin.getServer().getLogger().info(response.toString());
                }
            }

            if (playerList.length <= ++currentPlayerIndex) {
                playerList = null;
                currentPlayerIndex = -1;
                int count = 0;
                while (taxIntervalStart <= System.currentTimeMillis()) {
                    taxIntervalStart += taxPeriod;
                    ++count;
                }
                if (1 < count) {
                    plugin.getServer().getLogger()
                        .warning(String.format("[%s] Server overloaded or tickPeriod is too small.",
                                 plugin.getDescription().getName()));
                }
                scheduledTask = new TaxCollectorTask(this);
                scheduledTask.runTaskLater(plugin,
                        (taxIntervalStart - System.currentTimeMillis())
                                / tickInterval);

                return;
            }
        }

        if (isEnabled) {
            scheduledTask = new TaxCollectorTask(this);
            scheduledTask.runTask(plugin);
        }
    }

}
