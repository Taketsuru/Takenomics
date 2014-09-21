package jp.dip.myuminecraft.takenomics;

import java.util.IllformedLocaleException;
import java.util.Locale;

import jp.dip.myuminecraft.takenomics.chestshop.ChestShopMonitor;
import net.milkbowl.vault.economy.Economy;

import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.event.Listener;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.RegisteredServiceProvider;
import org.bukkit.plugin.java.JavaPlugin;

import com.sk89q.worldguard.bukkit.WorldGuardPlugin;

public class Takenomics extends JavaPlugin implements Listener {

    Logger                logger;
    Messages              messages;
    CommandDispatcher     commandDispatcher;
    SignScanner           signScanner;
    Database              database;
    PlayerMonitor         playerMonitor;
    TaxLogger             taxLogger;
    Economy               economy;
    WorldGuardPlugin      worldGuard;
    TaxOnSavingsCollector taxOnSavingsCollector;
    RedstoneTaxCollector  redstoneTaxCollector;
    private ChestShopMonitor chestShopMonitor;

    @Override
    public void onEnable() {
        try {
            saveDefaultConfig();
            
            logger = new Logger(getLogger());
            messages = new Messages(getLocale());
            
            commandDispatcher = new CommandDispatcher(messages, "takenomics.command.");
            getCommand("takenomics").setExecutor(commandDispatcher);

            registerReloadCommand();

            signScanner = new SignScanner(this, logger, messages, commandDispatcher);
            signScanner.enable();

            database = new Database(this, logger);
            if (! database.enable()) {
                logger.warning("Disable database access.");
                database = null;
            }

            if (database.getConnection() == null) {
                database = null;
            }

            playerMonitor = null;
            if (database != null) {
                playerMonitor = new PlayerMonitor(this, logger, database);
                if (! playerMonitor.enable()) {
                    logger.warning("Disable player and world list synchronization.");
                    playerMonitor = null;
                }
            }

            chestShopMonitor = null;
            if (playerMonitor != null) {
                chestShopMonitor = new ChestShopMonitor(this, logger, database, playerMonitor);
                if (! chestShopMonitor.enable()) {
                    logger.warning("Disable ChestShop and MySQL connector.");
                    chestShopMonitor = null;
                }
            }

            taxLogger = new TaxLogger(this);
            economy = getEconomyProvider();
            worldGuard = getWorldGuard();

            taxOnSavingsCollector = new TaxOnSavingsCollector(this, logger, messages, taxLogger, economy);
            taxOnSavingsCollector.enable();

            redstoneTaxCollector = new RedstoneTaxCollector(this, logger, messages, taxLogger, economy, worldGuard);
            redstoneTaxCollector.enable();
        } catch (Throwable th) {
            getLogger().severe(th.toString());
            for (StackTraceElement e : th.getStackTrace()) {
                getLogger().severe(e.toString());
            }
            getServer().getPluginManager().disablePlugin(this);
        }
    }
    
    void registerReloadCommand() {
        commandDispatcher.addCommand("reload", new CommandExecutor() {
            public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
                if (args.length != 1) {
                    return false;
                }
                reload();
                logger.info("Finished reloading %s", getName());

                return true;
            }
        }); 
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
        if (redstoneTaxCollector != null) {
            redstoneTaxCollector.disable();
            redstoneTaxCollector = null;
        }

        if (taxOnSavingsCollector != null) {
            taxOnSavingsCollector.disable();
            taxOnSavingsCollector = null;
        }

        worldGuard = null;
        economy = null;

        if (taxLogger != null) {
            taxLogger.disable();
            taxLogger = null;
        }

        if (database != null) {
            database.disable();
            database = null;
        }

        signScanner = null;
        commandDispatcher = null;     
        messages = null;
        logger = null;
    }

    public void reload() {
        getServer().getPluginManager().disablePlugin(this);
        getServer().getPluginManager().enablePlugin(this);        
    }
}
