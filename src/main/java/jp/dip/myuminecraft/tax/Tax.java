package jp.dip.myuminecraft.tax;

import java.util.IllformedLocaleException;
import java.util.Locale;

import org.bukkit.configuration.file.FileConfiguration;
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
            
            FileConfiguration config = getConfig();

            String languageTag = config.getString("locale");
            locale = null;
            if (languageTag == null) {
                getLogger().warning(String.format("[%s] Can't find locale configurations.", getName()));
            } else {            
                try {
                    locale = new Locale.Builder().setLanguageTag(languageTag).build();
                } catch (IllformedLocaleException e) {
                    getLogger().warning(String.format("[%s] Illegal locale '%s' is specified.", getName(), languageTag));
               }
            }
            if (locale == null) {
                locale = new Locale("en-US");               
            }

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
