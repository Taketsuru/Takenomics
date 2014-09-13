package jp.dip.myuminecraft.takenomics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bukkit.configuration.file.FileConfiguration;

public class TaxTable {

    class TaxClass {
        double min;
        double rate;

        TaxClass(double min, double rate) {
            this.min = min;
            this.rate = rate;
        }        
    }

    List<TaxClass> classes = new ArrayList<TaxClass>();
    
    public TaxTable() {
    }

    public ArrayList<String> loadConfig(String configPrefix, FileConfiguration config) {
        classes.clear();
        
        ArrayList<String> result = new ArrayList<String>();

        String configClasses = configPrefix + ".table";

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> classConfig =
        (List<Map<String, Object>>) config.getList(configClasses);
        if (classConfig == null) {
            result.add(String.format("No '%s' configurations.", configClasses));
        }

        if (result.isEmpty()) {
            int index = 0;
            for (Map<String, Object> entry : classConfig) {

                Object min = entry.get("min");

                if (min == null) {
                    result.add(String.format("%d-th row of '%s' doesn't have 'min' field.",
                            index + 1, configClasses));
                } else if (! (min instanceof Number)) {
                    result.add(String.format("'min' field of %d-th row of '%s' has an invalid value.",
                            index + 1, configClasses));
                    min = null;
                }

                Object rate = entry.get("rate");

                if (rate == null) {
                    result.add(String.format("%d-th row of '%s' doesn't have 'rate' field.",
                            index + 1, configClasses));
                } else if (! (rate instanceof Number)) {
                    result.add(String.format("'rate' field of %d-th row of '%s' has an invalid value.",
                            index + 1, configClasses));
                    rate = null;
                }

                if (min != null && rate != null) {
                    classes.add(new TaxClass(((Number)min).doubleValue(),
                            ((Number)rate).doubleValue()));
                }

                ++index;
            }
        }

        if (! result.isEmpty()) {
            classes.clear();
        }
        
        return result;
    }

    public double getRate(double value) {
        double rate = 0.0;
        for (TaxClass c : classes) {
            if (value < c.min) {
                break;
            }
            rate = c.rate;
        } 
        
        return rate;
    }
    
    public void clear() {
        classes.clear();
    }
}
