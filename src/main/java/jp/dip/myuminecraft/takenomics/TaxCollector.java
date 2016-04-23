package jp.dip.myuminecraft.takenomics;

import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.plugin.java.JavaPlugin;

import jp.dip.myuminecraft.takecore.Logger;

public class TaxCollector {

    protected JavaPlugin plugin;
    protected Logger     logger;
    protected boolean    enable = false;
    protected boolean    debug  = false;

    public TaxCollector(JavaPlugin plugin, Logger logger) {
        this.plugin = plugin;
        this.logger = logger;
    }

    protected void loadConfig(Logger logger, FileConfiguration config, String configPrefix, String taxName) {
        if (loadConfig(logger, plugin.getConfig(), configPrefix, false)) {
            logger.warning("Disable %s.", taxName);
            enable = false;
        }        
    }

    protected boolean loadConfig(Logger logger, FileConfiguration config, String configPrefix, boolean error) {
        String configEnable = configPrefix + ".enable";
        String configDebug = configPrefix + ".debug";

        boolean result = error;

        enable = false;
        if (! config.contains(configEnable)) {
            logger.warning("'%s' is not configured.", configEnable);
            result = true;
        } else if (! config.isBoolean(configEnable)) {
            logger.warning("'%s' is not a boolean.", configEnable);
            result = true;
        } else {
            enable = config.getBoolean(configEnable);
        }

        debug = false;
        if (config.contains(configDebug)) {
            if (! config.isBoolean(configDebug)) {
                logger.warning("'%s' is not a boolean.", configDebug);
            } else {
                debug = config.getBoolean(configDebug);
            }
        }

        return result;
    }
}
