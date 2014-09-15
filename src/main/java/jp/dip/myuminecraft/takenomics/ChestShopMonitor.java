package jp.dip.myuminecraft.takenomics;

import org.bukkit.event.Listener;
import org.bukkit.plugin.java.JavaPlugin;

public class ChestShopMonitor implements Listener {

    private JavaPlugin plugin;
    private Logger logger;
    private Messages messages;
    private Database database;

    public ChestShopMonitor(JavaPlugin plugin, Logger logger, Messages messages, Database database) {
        this.plugin = plugin;
        this.logger = logger;
        this.messages = messages;
        this.database = database;
    }
    
    public boolean enable() {
        
        return true;
    }
}
