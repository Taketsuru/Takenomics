package jp.dip.myuminecraft.takenomics;

import java.util.IllformedLocaleException;
import java.util.Locale;

import net.milkbowl.vault.economy.Economy;

import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.event.Listener;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.RegisteredServiceProvider;
import org.bukkit.plugin.java.JavaPlugin;

import com.sk89q.worldguard.bukkit.WorldGuardPlugin;

public class Takenomics extends JavaPlugin implements Listener {

    Logger                logger;
    Messages              messages;
    Economy               economy;
    TaxLogger             taxLogger;
    TaxOnSavingsCollector taxOnSavingsCollector;
    RedstoneTaxCollector  redstoneTaxCollector;
    WorldGuardPlugin      worldGuard;

    @Override
    public void onEnable() {
        try {
            saveDefaultConfig();
            
            logger = new Logger(getLogger());
            messages = new Messages(getLocale());
            taxLogger = new TaxLogger(this);
            economy = getEconomyProvider();
            worldGuard = getWorldGuard();

            taxOnSavingsCollector = new TaxOnSavingsCollector(this, logger, messages, taxLogger, economy);
            redstoneTaxCollector = new RedstoneTaxCollector(this, logger, messages, taxLogger, economy, worldGuard);

            taxOnSavingsCollector.enable();
            redstoneTaxCollector.enable();
        } catch (Throwable th) {
            getLogger().severe(th.toString());
            for (StackTraceElement e : th.getStackTrace()) {
                getLogger().severe(e.toString());
            }
            getServer().getPluginManager().disablePlugin(this);
        }
    }

    Locale getLocale() {
        FileConfiguration config = getConfig();

        String languageTag = config.getString("locale");
        Locale result = null;
        if (languageTag == null) {
            logger.warning("Can't find locale configurations.");
        } else {            
            try {
                result = new Locale.Builder().setLanguageTag(languageTag).build();
            } catch (IllformedLocaleException e) {
                logger.warning("Illegal locale '%s' is specified.", getName(), languageTag);
           }
        }
        if (result == null) {
            result = new Locale("en-US");               
        }
        
        return result;
    }

    Economy getEconomyProvider() throws Exception {
        RegisteredServiceProvider<Economy> economyProvider = getServer()
                .getServicesManager()
                .getRegistration(net.milkbowl.vault.economy.Economy.class);

        if (economyProvider == null) {
            String msg = String.format("No Vault found!  Disabled %s.", getName());
            throw new Exception(msg);
        }

        return economyProvider.getProvider();
    }

    WorldGuardPlugin getWorldGuard() throws Exception {
        Plugin result = getServer().getPluginManager().getPlugin("WorldGuard");

        if (result == null || ! (result instanceof WorldGuardPlugin)) {
            String msg = String.format("No WorldGuard found!  Disabled %s.", getName());
            throw new Exception(msg);
        }
     
        return (WorldGuardPlugin) result;

    }
    
    @Override
    public void onDisable() {
        messages = null;
        if (taxOnSavingsCollector != null) {
            taxOnSavingsCollector.disable();
            taxOnSavingsCollector = null;
        }
        if (redstoneTaxCollector != null) {
            redstoneTaxCollector.disable();
            redstoneTaxCollector = null;
        }
        if (taxLogger != null) {
            taxLogger.disable();
            taxLogger = null;
        }
    }

}
