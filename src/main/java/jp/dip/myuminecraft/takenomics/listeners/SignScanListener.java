package jp.dip.myuminecraft.takenomics.listeners;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import jp.dip.myuminecraft.takecore.Logger;
import jp.dip.myuminecraft.takenomics.Database;
import jp.dip.myuminecraft.takenomics.SignScanEvent;
import jp.dip.myuminecraft.takenomics.models.ShopTable;

import org.bukkit.Chunk;
import org.bukkit.block.BlockState;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.plugin.java.JavaPlugin;

public class SignScanListener implements Listener {
    
    JavaPlugin plugin;
    Logger     logger;
    Database   database;
    ShopTable  shopTable;

    public SignScanListener(JavaPlugin plugin, Logger logger,
            Database database, ShopTable shopTable) {
        this.plugin = plugin;
        this.logger = logger;
        this.database = database;
        this.shopTable = shopTable;
    }

    public boolean enable() {
        if (shopTable == null) {
            return true;
        }

        return false;
    }

    @EventHandler
    public void onSignScanEvent(SignScanEvent event) {
        try {
            Chunk chunk = event.getChunk();
            final int chunkX = chunk.getX();
            final int chunkZ = chunk.getZ();
            final Collection<BlockState> states =
                    new ArrayList<BlockState>(event.getSigns());
 
            database.runAsynchronously(new Runnable() {
                public void run() {
                    try {
                        shopTable.runSyncChunkTransaction(chunkX, chunkZ, states);
                    } catch (SQLException e) {
                        logger.warning(e, "exception in sign scanning.");
                    }
                }
            });
        } catch (Exception e) {
            logger.warning(e, "Failed to scan signs.");
        }
    }

}
