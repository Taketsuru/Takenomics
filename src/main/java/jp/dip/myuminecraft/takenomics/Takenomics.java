package jp.dip.myuminecraft.takenomics;

import java.sql.Connection;
import java.util.Locale;
import java.util.ResourceBundle;

import jp.dip.myuminecraft.takecore.DatabaseTask;
import jp.dip.myuminecraft.takecore.Logger;
import jp.dip.myuminecraft.takecore.Messages;
import jp.dip.myuminecraft.takenomics.listeners.PlayerJoinQuitListener;
import jp.dip.myuminecraft.takenomics.listeners.UserBalanceUpdateListener;
import jp.dip.myuminecraft.takenomics.models.AccessLog;
import jp.dip.myuminecraft.takenomics.models.PlayerTable;
import jp.dip.myuminecraft.takenomics.models.ShopTable;
import jp.dip.myuminecraft.takenomics.models.TransactionTable;
import jp.dip.myuminecraft.takenomics.models.WorldTable;
import net.milkbowl.vault.economy.Economy;

import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.plugin.java.JavaPlugin;

public class Takenomics extends JavaPlugin {

    private Logger                logger;
    private Messages              messages;
    private CommandDispatcher     commandDispatcher;
    private SignScanner           signScanner;
    private Database              database;
    private PlayerTable           playerTable;
    private WorldTable            worldTable;
    private AccessLog             accessLog;
    private ShopTable             shopTable;
    private TransactionTable      transactionTable;
    private TaxOnSavingsCollector taxOnSavingsCollector;
    private RedstoneTaxCollector  redstoneTaxCollector;
    private ShopMonitor           chestShopMonitor;
    private LivestockTaxCollector livestockTaxCollector;
    private HopperTaxCollector    hopperTaxCollector;
    private EssentialsShopMonitor essentialsShopMonitor;
    private LandRentalManager     landRentalManager;
    private LandSelectionManager  landSelectionManager;

    @Override
    public void onEnable() {
        try {
            saveDefaultConfig();

            logger = new Logger(getLogger());
            Locale locale = Messages
                    .getLocale(getConfig().getString("locale"));
            messages = new Messages(
                    ResourceBundle.getBundle("messages", locale), locale);

            database = new Database(logger);
            database.enable(getConfig().getConfigurationSection("database"));

            // Vault
            Economy economy = Bukkit.getServicesManager()
                    .getRegistration(net.milkbowl.vault.economy.Economy.class)
                    .getProvider();

            commandDispatcher = new CommandDispatcher(messages,
                    "takenomics.command.");
            getCommand("takenomics").setExecutor(commandDispatcher);

            registerReloadCommand();

            signScanner = new SignScanner(this, logger, messages,
                    commandDispatcher);
            signScanner.enable();

            playerTable = new PlayerTable(this, logger, database, economy);
            if (playerTable.enable()) {
                playerTable = null;
            } else {
                Bukkit.getPluginManager()
                        .registerEvents(new UserBalanceUpdateListener(this,
                                logger, database, playerTable), this);
            }

            worldTable = new WorldTable(this, logger, database);
            if (worldTable.enable()) {
                worldTable = null;
            }

            accessLog = new AccessLog(this, logger, database, playerTable);
            if (accessLog.enable()) {
                accessLog = null;
            } else {
                Bukkit.getPluginManager().registerEvents(
                        new PlayerJoinQuitListener(logger, accessLog), this);
            }

            shopTable = new ShopTable(this, logger, database, playerTable,
                    worldTable);
            if (shopTable.enable()) {
                shopTable = null;
            } else {
                registerShopCommands();
            }

            transactionTable = new TransactionTable(this, logger, database,
                    playerTable, shopTable);
            if (transactionTable.enable()) {
                transactionTable = null;
            }

            chestShopMonitor = new ShopMonitor(this, logger, database,
                    playerTable, worldTable, shopTable, transactionTable);
            if (chestShopMonitor.enable()) {
                chestShopMonitor = null;
            }

            essentialsShopMonitor = new EssentialsShopMonitor(this, logger,
                    database, shopTable, transactionTable);
            if (essentialsShopMonitor.enable()) {
                essentialsShopMonitor = null;
            }

            landSelectionManager = new LandSelectionManager(this, logger,
                    messages);
            landSelectionManager.enable();

            if (database != null && worldTable != null
                    && playerTable != null) {
                landRentalManager = new LandRentalManager(this, logger,
                        messages, economy, database, playerTable, worldTable,
                        landSelectionManager);
                landRentalManager.enable();
            }

            taxOnSavingsCollector = new TaxOnSavingsCollector(this, logger,
                    messages, economy);
            taxOnSavingsCollector.enable();

            redstoneTaxCollector = new RedstoneTaxCollector(this, logger,
                    messages, economy, landRentalManager);
            redstoneTaxCollector.enable();

            livestockTaxCollector = new LivestockTaxCollector(this, logger,
                    messages, database, economy, landRentalManager);
            livestockTaxCollector.enable();

            hopperTaxCollector = new HopperTaxCollector(this, logger, messages,
                    economy, landRentalManager);
            hopperTaxCollector.enable();

        } catch (Throwable th) {
            logger.warning(th, "Failed to initialize");
            getServer().getPluginManager().disablePlugin(this);
        }
    }

    void registerReloadCommand() {
        commandDispatcher.addCommand("reload", new CommandExecutor() {
            public boolean onCommand(CommandSender sender, Command cmd,
                    String label, String[] args) {
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
        CommandDispatcher shopCommands = new CommandDispatcher(
                commandDispatcher, "shops");

        shopCommands.addCommand("scrub", new CommandExecutor() {
            @Override
            public boolean onCommand(CommandSender sender, Command arg1,
                    String arg2, String[] arg3) {
                if (shopTable == null) {
                    return true;
                }

                try {
                    database.submitAsync(new DatabaseTask() {
                        @Override
                        public void run(Connection connection)
                                throws Throwable {
                            shopTable.runScrubTransaction(connection);
                        }
                    });
                } catch (Throwable e) {
                    logger.warning(e, "failed to scrub shop table.");
                }
                return true;
            }
        });
    }

    @Override
    public void onDisable() {
        if (landRentalManager != null) {
            landRentalManager.disable();
            landRentalManager = null;
        }

        if (hopperTaxCollector != null) {
            hopperTaxCollector.disable();
            hopperTaxCollector = null;
        }

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
