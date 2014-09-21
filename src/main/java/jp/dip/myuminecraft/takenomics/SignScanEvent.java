package jp.dip.myuminecraft.takenomics;

import java.util.List;

import org.bukkit.Chunk;
import org.bukkit.block.Block;
import org.bukkit.event.Event;
import org.bukkit.event.HandlerList;

public class SignScanEvent extends Event {
    
    private static final HandlerList handlers = new HandlerList();
    Chunk                            chunk;
    List<Block>                      signs;

    public HandlerList getHandlers() {
        return handlers;
    }
     
    public static HandlerList getHandlerList() {
        return handlers;
    }
    
    public Chunk getChunk() {
        return chunk;
    }
    
    public List<Block> getSigns() {
        return signs;
    }

    SignScanEvent(Chunk chunk, List<Block> signs) {
        this.chunk = chunk;
        this.signs = signs;
    }
}
