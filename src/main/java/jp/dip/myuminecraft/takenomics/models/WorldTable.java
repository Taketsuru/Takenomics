package jp.dip.myuminecraft.takenomics.models;

import java.sql.SQLException;
import java.util.Iterator;

import jp.dip.myuminecraft.takenomics.Database;
import jp.dip.myuminecraft.takenomics.Logger;

import org.bukkit.World;
import org.bukkit.plugin.java.JavaPlugin;

public class WorldTable extends IndexedStringTable {

    public WorldTable(JavaPlugin plugin, Logger logger, Database database) {
        super(plugin, logger, database, "worlds", "TINYINT", "VARCHAR(50) CHARACTER SET ascii");
    }
    
    public boolean enable() {
        try {
            super.enable(new Iterator<String>() {
                Iterator<World> slave = plugin.getServer().getWorlds().iterator();

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
            logger.warning(e, "Failed to initialize worlds table.");
            disable();
            return false;
        }

        return true;
    }
    
    public int getId(World world) throws SQLException {
        return getId(world.getName());
    }       
}
