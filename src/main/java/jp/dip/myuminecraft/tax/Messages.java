package jp.dip.myuminecraft.tax;

import java.io.UnsupportedEncodingException;
import java.util.Locale;
import java.util.ResourceBundle;

public class Messages {
    
    private ResourceBundle bundle;

    public Messages(Locale locale) {
            bundle = ResourceBundle.getBundle("messages", locale);
    }

    public String getString(String key) {
        try {
            return new String(bundle.getString(key).getBytes("ISO-8859-1"), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new Error(e);
        }
    }

}
