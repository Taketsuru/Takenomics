package jp.dip.myuminecraft.takenomics;

import java.io.UnsupportedEncodingException;
import java.util.Locale;
import java.util.ResourceBundle;

import org.bukkit.entity.Player;

public class Messages {

    ResourceBundle bundle;
    Locale         locale;

    public Messages(Locale locale) {
            bundle = ResourceBundle.getBundle("messages", locale);
            this.locale = locale;
    }

    public String getString(String key) {
        try {
            return new String(bundle.getString(key).getBytes("ISO-8859-1"), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new Error(e);
        }
    }

    public void chat(Player player, String formatKey, Object...args) {
        player.sendMessage(String.format(locale, getString(formatKey), args));
    }

}
