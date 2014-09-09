package jp.dip.myuminecraft.takenomics;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.bukkit.OfflinePlayer;

public abstract class TaxRecord {
    
    public TaxRecord(long timestamp, OfflinePlayer player) {
        this.timestamp = timestamp;
        this.player = player;
    }
    
    public String timestampToString() {
        return timestampFormat.format(new Date(timestamp));
    }
    
    protected abstract String subclassToString();

    public String toString() {
        return String.format("%s %s %s",
                timestampToString(),
                player.getName(),
                subclassToString());
    }

    static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long                          timestamp;
    OfflinePlayer                 player;
}