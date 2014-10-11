package jp.dip.myuminecraft.takenomics;

import java.sql.SQLException;
import java.util.IllformedLocaleException;
import java.util.Locale;

import jp.dip.myuminecraft.takenomics.listeners.PlayerJoinQuitListener;
import jp.dip.myuminecraft.takenomics.listeners.SignScanListener;
import jp.dip.myuminecraft.takenomics.models.AccessLog;
import jp.dip.myuminecraft.takenomics.models.PlayerTable;
import jp.dip.myuminecraft.takenomics.models.ShopTable;
import jp.dip.myuminecraft.takenomics.models.TransactionTable;
import jp.dip.myuminecraft.takenomics.models.WorldTable;
import net.milkbowl.vault.economy.Economy;

import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.RegisteredServiceProvider;
import org.bukkit.plugin.java.JavaPlugin;

import com.sk89q.worldguard.bukkit.WorldGuardPlugin;

public class Takenomics extends JavaPlugin {

    Logger                logger;
    Messages              messages;
    CommandDispatcher     commandDispatcher;
    SignScanner           signScanner;
    Database              database;
    PlayerTable           playerTable;
    WorldTable            worldTable;
    AccessLog             accessLog;
    ShopTable             shopTable;
    TransactionTable      transactionTable;
    TaxLogger             taxLogger;
    Economy               economy;
    WorldGuardPlugin      worldGuard;
    RegionManager         regionManager;
    TaxOnSavingsCollector taxOnSavingsCollector;
    RedstoneTaxCollector  redstoneTaxCollector;
    ShopMonitor           chestShopMonitor;
    LivestockTaxCollector livestockTaxCollector;

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
            if (database.enable() || database.getConnection() == null) {
                database = null;
            }

            playerTable = new PlayerTable(this, logger, database);
            if (playerTable.enable()) {
                playerTable = null;
            }
 
            worldTable = new WorldTable(this, logger, database);
            if (worldTable.enable()) {
                worldTable = null;
            }

            accessLog = new AccessLog(this, logger, database, playerTable);
            if (accessLog.enable()) {
                accessLog = null;
            } else {
                getServer().getPluginManager().registerEvents(
                        new PlayerJoinQuitListener(logger, playerTable, accessLog), this);
            }

            shopTable = new ShopTable(this, logger, database, playerTable, worldTable);
            if (shopTable.enable()) {
                shopTable = null;
            } else {
                getServer().getPluginManager().registerEvents(
                        new SignScanListener(this, logger, database, shopTable), this);                
                registerShopCommands();
            }
            
            transactionTable = new TransactionTable(this, logger, database, playerTable, shopTable);
            if (transactionTable.enable()) {
                transactionTable = null;
            }

            chestShopMonitor = new ShopMonitor
                    (this, logger, database, playerTable, worldTable, shopTable, transactionTable);
            if (chestShopMonitor.enable()) {
                chestShopMonitor = null;
            }

            taxLogger = new TaxLogger(this);
            economy = getEconomyProvider();
            worldGuard = getWorldGuard();
            regionManager = new RegionManager(this, logger, playerTable, worldGuard);

            taxOnSavingsCollector = new TaxOnSavingsCollector
                    (this, logger, messages, taxLogger, economy);
            taxOnSavingsCollector.enable();

            redstoneTaxCollector = new RedstoneTaxCollector
                    (this, logger, messages, taxLogger, economy, regionManager);
            redstoneTaxCollector.enable();
            
            livestockTaxCollector = new LivestockTaxCollector
                    (this, logger, messages, database, taxLogger, regionManager, economy);
            livestockTaxCollector.enable();

        } catch (Throwable th) {
            logger.warning(th, "Failed to initialize %s", getName());
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

    void registerShopCommands() {
        CommandDispatcher shopCommands= new CommandDispatcher(commandDispatcher, "shops");

        shopCommands.addCommand("scrub", new CommandExecutor() {
            @Override
            public boolean onCommand(CommandSender sender, Command arg1,
                    String arg2, String[] arg3) {
                try {
                    shopTable.runScrubTransaction();
                } catch (SQLException e) {
                    logger.warning(e, "failed to scrub shop table.");
                }
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
        if (livestockTaxCollector != null) {
            livestockTaxCollector.disable();
            livestockTaxCollector = null;
        }

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
