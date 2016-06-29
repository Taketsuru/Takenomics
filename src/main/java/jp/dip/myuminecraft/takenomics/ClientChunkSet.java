package jp.dip.myuminecraft.takenomics;

import java.util.HashSet;
import java.util.Set;

public class ClientChunkSet {

    private static class ChunkId {
        int x;
        int z;

        public ChunkId(int x, int z) {
            this.x = x;
            this.z = z;
        }

        public boolean equals(Object obj) {
            if (!(obj instanceof ChunkId)) {
                return false;
            }
            ChunkId cobj = (ChunkId) obj;
            return x == cobj.x && z == cobj.z;
        }

        public int hashCode() {
            int acc = 0;
            int accx = x;
            int accz = z;
            for (int i = 0; i < 32; ++i) {
                acc <<= 2;
                acc |= ((accx & 1) << 1) | (accz & 1);
                acc ^= (((accx >> 16) & 1) << 1) | ((accz >> 16) & 1);
                accx <<= 2;
                accz <<= 2;
            }
            return acc;
        }
    }

    private Set<ChunkId> chunks = new HashSet<ChunkId>();

    public ClientChunkSet() {
    }

    public void onLoad(int x, int z) {
        chunks.add(new ChunkId(x, z));
    }

    public void onUnload(int x, int z) {
        chunks.remove(new ChunkId(x, z));
    }
    
    public boolean isLoaded(int x, int z) {
        return chunks.contains(new ChunkId(x, z));
    }

    public void clear() {
        chunks.clear();
    }

}
