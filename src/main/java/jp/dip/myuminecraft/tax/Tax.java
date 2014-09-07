package jp.dip.myuminecraft.tax;

import org.bukkit.event.Listener;
import org.bukkit.plugin.java.JavaPlugin;

public class Tax extends JavaPlugin implements Listener {
        
    TaxCollector collector;

    @Override
    public void onEnable() {
        try {
            collector = new TaxCollector(this);
        } catch (Exception exception) {
            getLogger().severe(exception.toString());
            getServer().getPluginManager().disablePlugin(this);
        }
    }

    @Override
    public void onDisable() {
        collector.disable();
        collector = null;
    }

}
