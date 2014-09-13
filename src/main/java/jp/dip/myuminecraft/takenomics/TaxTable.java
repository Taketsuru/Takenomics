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

    public boolean loadConfig(Logger logger, FileConfiguration config, String configPrefix) {
        classes.clear();
        
        boolean result = true;

        String configClasses = configPrefix + ".table";

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> classConfig =
        (List<Map<String, Object>>) config.getList(configClasses);
        if (classConfig == null) {
            logger.warning("No '%s' configurations.", configClasses);
            result = false;
        } else {
            int index = 0;
            for (Map<String, Object> entry : classConfig) {

                Object min = entry.get("min");

                if (min == null) {
                    logger.warning("%d-th row of '%s' doesn't have 'min' field.",
                            index + 1, configClasses);
                    result = false;
                } else if (! (min instanceof Number)) {
                    logger.warning("'min' field of %d-th row of '%s' has an invalid value.",
                            index + 1, configClasses);
                    result = false;
                }

                Object rate = entry.get("rate");

                if (rate == null) {
                    logger.warning("%d-th row of '%s' doesn't have 'rate' field.",
                            index + 1, configClasses);
                    result = false;
                } else if (! (rate instanceof Number)) {
                    logger.warning("'rate' field of %d-th row of '%s' has an invalid value.",
                            index + 1, configClasses);
                    result = false;
                }

                if (result) {
                    classes.add(new TaxClass(((Number)min).doubleValue(),
                            ((Number)rate).doubleValue()));
                }

                ++index;
            }
        }

        if (! result) {
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
    
    public double getTaxExemptionLimit() {
        return  classes.isEmpty() ? 0.0 : classes.get(0).min;
    }
    
    public void clear() {
        classes.clear();
    }
}
