package jp.dip.myuminecraft.tax;

import java.util.Locale;

import org.bukkit.event.Listener;
import org.bukkit.plugin.java.JavaPlugin;

public class Tax extends JavaPlugin implements Listener {

    Locale       locale;
    Messages     messages;
    TaxCollector collector;

    @Override
    public void onEnable() {
        try {
            saveDefaultConfig();
            String language = "ja";
            String country = "JP";
            locale = new Locale(language, country);
            messages = new Messages(locale);
            collector = new TaxCollector(this, messages);
        } catch (Throwable th) {
            getLogger().severe(th.toString());
            getServer().getPluginManager().disablePlugin(this);
        }
    }

    @Override
    public void onDisable() {
        locale = null;
        messages = null;
        if (collector != null) {
            collector.disable();
            collector = null;
        }
    }

}
