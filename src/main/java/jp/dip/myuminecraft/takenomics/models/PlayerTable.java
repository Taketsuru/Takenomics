package jp.dip.myuminecraft.takenomics.models;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;

import jp.dip.myuminecraft.takenomics.Database;
import jp.dip.myuminecraft.takenomics.Logger;

import org.bukkit.OfflinePlayer;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;

public class PlayerTable extends IndexedStringTable {

    public PlayerTable(JavaPlugin plugin, Logger logger, Database database) {
        super(plugin, logger, database, "players", "MEDIUMINT", "VARCHAR(16) CHARACTER SET ascii");
    }
    
    public boolean enable() {
        try {
            super.enable(new Iterator<String>() {
                Iterator<Player> slave = Arrays.asList(plugin.getServer().getOnlinePlayers()).iterator();
                
                public boolean hasNext() {
                    return slave.hasNext();
                }
                
                public String next() {
                    return slave.next().getName();
                }
                
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            });

        } catch (SQLException e) {
            logger.warning(e, "Failed to initialize players table.");
            disable();
            return false;
        }

        return true;
    }

    public int getId(OfflinePlayer player) throws SQLException {
        return super.getId(player.getName());
    }

    public void mayDropCache(Player player) {
        super.mayDropCache(player.getName());
    }
    
}
