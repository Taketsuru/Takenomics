package jp.dip.myuminecraft.takenomics;

public class Constants {
    public final static int ticksPerSecond     = 20;
    public final static int chunkXSize         = 16;
    public final static int chunkYSize         = 256;
    public final static int chunkZSize         = 16;
    public static final int sizeOfInt          = 4;
    public static final int log2SectorSize     = 12;
    public static final int regionXSize        = 32;
    public static final int regionZSize        = 32;
    public static final int chunksPerRegion    = regionXSize * regionZSize;
    public static final int chunkLocationsSize = sizeOfInt * chunksPerRegion;
    public static final int maxChunkSize       = 1024 * 1024;
    
    public static final int sizeOfUUID = 16;
}
