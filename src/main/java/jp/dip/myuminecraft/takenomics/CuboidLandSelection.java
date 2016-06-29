package jp.dip.myuminecraft.takenomics;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.bukkit.DyeColor;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.entity.Player;
import org.bukkit.event.player.PlayerInteractEvent;
import org.bukkit.event.player.PlayerMoveEvent;

import com.comphenix.protocol.PacketType;
import com.comphenix.protocol.ProtocolLibrary;
import com.comphenix.protocol.ProtocolManager;
import com.comphenix.protocol.events.PacketContainer;
import com.comphenix.protocol.wrappers.ChunkCoordIntPair;
import com.comphenix.protocol.wrappers.MultiBlockChangeInfo;
import com.comphenix.protocol.wrappers.WrappedBlockData;
import com.sk89q.worldedit.BlockVector;
import com.sk89q.worldguard.protection.regions.ProtectedCuboidRegion;
import com.sk89q.worldguard.protection.regions.ProtectedRegion;

import jp.dip.myuminecraft.takecore.BlockCoordinates;
import jp.dip.myuminecraft.takecore.BoundingBox;
import jp.dip.myuminecraft.takecore.Logger;

public class CuboidLandSelection implements LandSelection {

    private Logger                 logger;
    private ProtocolManager        protocolManager;
    private Player                 player;
    private World                  world;
    private Block                  lastTarget;
    private Block                  lastPlayerBlock;
    private List<BlockCoordinates> controlPoints  = new ArrayList<>();
    private BoundingBox            currentGuide   = new BoundingBox();
    private Set<BlockCoordinates>  guideBlocks    = new HashSet<>();
    private WrappedBlockData[]     guideMaterials = new WrappedBlockData[2];

    @SuppressWarnings({ "deprecation" })
    public CuboidLandSelection(Logger logger, Player player) {
        this.logger = logger;
        this.player = player;
        this.world = player.getWorld();
        guideMaterials[0] = WrappedBlockData.createData(Material.STAINED_GLASS,
                DyeColor.CYAN.getData());
        guideMaterials[1] = WrappedBlockData.createData(Material.STAINED_GLASS,
                DyeColor.WHITE.getData());
        protocolManager = ProtocolLibrary.getProtocolManager();
    }

    @Override
    public void clear() {
        lastTarget = null;
        setGuide(new BoundingBox());
        controlPoints.clear();
        currentGuide.clear();
        guideBlocks.clear();
    }

    @Override
    public BlockProximity getBlockProximity(BlockCoordinates coords) {
        if (!isSelected()) {
            return BlockProximity.OFF;
        }

        int x = coords.getX();
        int y = coords.getY();
        int z = coords.getZ();
        int minx = currentGuide.getMinX();
        int maxx = currentGuide.getMaxX();
        int miny = currentGuide.getMinY();
        int maxy = currentGuide.getMaxY();
        int minz = currentGuide.getMinZ();
        int maxz = currentGuide.getMaxZ();

        return minx <= x && x < maxx && miny <= y && y < maxy && minz <= z
                && z < maxz
                        ? BlockProximity.ON
                        : minx - 1 <= x && x <= maxx && miny - 1 <= y
                                && y <= maxy && minz - 1 <= z && z <= maxz
                                        ? BlockProximity.ONE_BLOCK_OFF
                                        : BlockProximity.OFF;
    }

    @Override
    public boolean isSelected() {
        return controlPoints.size() == 2;
    }

    @Override
    public ProtectedRegion getEnclosingRegion() {
        if (!isSelected()) {
            return null;
        }

        BoundingBox box = new BoundingBox();
        box.set(controlPoints);

        // (minx, miny, minz): 0
        Location loc = new Location(world, box.getMinX(),
                box.getMinY(), box.getMinZ());
        ProtectedRegion region = RegionUtil.getHighestPriorityRegion(loc);
        if (region == null) {
            return null;
        }

        // (maxx, miny, minz): 4
        loc.setX(box.getMaxX() - 1);
        ProtectedRegion r = RegionUtil.getHighestPriorityRegion(loc);
        if (r != region) {
            return null;
        }

        // (maxx, maxy, minz): 6
        loc.setY(box.getMaxY() - 1);
        r = RegionUtil.getHighestPriorityRegion(loc);
        if (r != region) {
            return null;
        }

        // (maxx, maxy, maxz): 7
        loc.setZ(box.getMaxZ() - 1);
        r = RegionUtil.getHighestPriorityRegion(loc);
        if (r != region) {
            return null;
        }

        // (minx, maxy, maxz): 3
        loc.setX(box.getMinX());
        r = RegionUtil.getHighestPriorityRegion(loc);
        if (r != region) {
            return null;
        }

        // (minx, miny, maxz): 1
        loc.setY(box.getMinY());
        r = RegionUtil.getHighestPriorityRegion(loc);
        if (r != region) {
            return null;
        }

        // (maxx, miny, maxz): 5
        loc.setY(box.getMinY());
        r = RegionUtil.getHighestPriorityRegion(loc);
        if (r != region) {
            return null;
        }

        // (minx, maxy, minz): 2
        loc.setX(box.getMinX());
        loc.setY(box.getMaxY() - 1);
        loc.setZ(box.getMinZ());
        r = RegionUtil.getHighestPriorityRegion(loc);
        if (r != region) {
            return null;
        }

        return region;
    }

    @Override
    public ProtectedRegion createRegionFromSelection(String id) {
        if (controlPoints.size() < 2) {
            return null;
        }

        BoundingBox box = new BoundingBox();
        box.set(controlPoints);

        BlockVector coords1 = new BlockVector(box.getMinX(), box.getMinY(),
                box.getMinZ());
        BlockVector coords2 = new BlockVector(box.getMaxX() - 1,
                box.getMaxY() - 1, box.getMaxZ() - 1);

        return new ProtectedCuboidRegion(id, coords1, coords2);
    }

    @Override
    public int getSelectedVolume() {
        if (controlPoints.size() < 2) {
            return 0;
        }

        return (currentGuide.getMaxX() - currentGuide.getMinX())
                * (currentGuide.getMaxY() - currentGuide.getMinY())
                * (currentGuide.getMaxZ() - currentGuide.getMinZ());
    }

    @Override
    public void onPlayerInteract(PlayerInteractEvent event) {
        switch (event.getAction()) {
        case LEFT_CLICK_BLOCK:
        case LEFT_CLICK_AIR:
            onLeftClick();
            break;

        case RIGHT_CLICK_BLOCK:
        case RIGHT_CLICK_AIR:
            onRightClick();
            break;

        default:
            break;
        }
    }

    @Override
    public void onPlayerMove(PlayerMoveEvent event) {
        Block playerBlock = player.getLocation().getBlock();
        boolean moved = lastPlayerBlock == null
                || playerBlock.getX() != lastPlayerBlock.getX()
                || playerBlock.getY() != lastPlayerBlock.getY()
                || playerBlock.getZ() != lastPlayerBlock.getZ();

        if (controlPoints.size() != 1) {
            if (moved) {
                lastPlayerBlock = playerBlock;

                BoundingBox newGuide = new BoundingBox();
                newGuide.set(controlPoints);
                setGuide(newGuide);
            }

            return;
        }

        Block target = getTargetBlock();
        boolean targetChanged = lastTarget == null
                || target.getX() != lastTarget.getX()
                || target.getY() != lastTarget.getY()
                || target.getZ() != lastTarget.getZ();

        if (moved || targetChanged) {
            lastTarget = target;
            lastPlayerBlock = playerBlock;

            BoundingBox newGuide = new BoundingBox();
            newGuide.set(controlPoints);
            newGuide.add(target.getX(), target.getY(), target.getZ());
            setGuide(newGuide);
        }
    }

    private Block getTargetBlock() {
        return player.getTargetBlock((Set<Material>) null, 5);
    }

    private void onLeftClick() {
        Block block = getTargetBlock();
        BlockCoordinates coords = new BlockCoordinates(block.getX(),
                block.getY(), block.getZ());
        if (2 <= controlPoints.size()) {
            controlPoints.set(controlPoints.size() - 1, coords);
        } else {
            controlPoints.add(coords);
        }
        lastTarget = block;
        BoundingBox newGuide = new BoundingBox();
        newGuide.set(controlPoints);
        setGuide(newGuide);
    }

    private void onRightClick() {
        if (controlPoints.isEmpty()) {
            return;
        }

        controlPoints.remove(controlPoints.size() - 1);
        BoundingBox newGuide = new BoundingBox();
        newGuide.set(controlPoints);
        setGuide(newGuide);
    }

    private void setXGuide(Set<BlockCoordinates> newGuideBlocks,
            Map<BlockCoordinates, WrappedBlockData> updates, int x, int miny,
            int maxy, int minz, int maxz, int excminx, int excmaxx,
            int excminy, int excmaxy, int excminz, int excmaxz) {
        for (int y = miny; y < maxy; ++y) {
            for (int z = minz; z < maxz; ++z) {
                if (excminx <= x && x < excmaxx && excminy <= y && y < excmaxy
                        && excminz <= z && z < excmaxz) {
                    continue;
                }
                BlockCoordinates coords = new BlockCoordinates(x, y, z);
                newGuideBlocks.add(coords);
                if (!guideBlocks.contains(coords)) {
                    updates.put(coords, guideMaterials[(x ^ y ^ z) & 1]);
                }
            }
        }
    }

    private void setYGuide(Set<BlockCoordinates> newGuideBlocks,
            Map<BlockCoordinates, WrappedBlockData> updates, int minx,
            int maxx, int y, int minz, int maxz, int playerx, int playerz,
            int maxDistance, int excminx, int excmaxx, int excminy,
            int excmaxy, int excminz, int excmaxz) {
        for (int z = minz; z < maxz; ++z) {
            int distz = Math.abs(z - playerz);
            if (distz < maxDistance) {
                int tminx = Math.max(minx, playerx - (maxDistance - distz));
                int tmaxx = Math.min(maxx, playerx + (maxDistance - distz));
                for (int x = tminx; x < tmaxx; ++x) {
                    if (excminx <= x && x < excmaxx && excminy <= y
                            && y < excmaxy && excminz <= z && z < excmaxz) {
                        continue;
                    }

                    BlockCoordinates coords = new BlockCoordinates(x, y, z);
                    newGuideBlocks.add(coords);
                    if (!guideBlocks.contains(coords)) {
                        updates.put(coords, guideMaterials[(x ^ y ^ z) & 1]);
                    }
                }
            }
        }
    }

    private void setZGuide(Set<BlockCoordinates> newGuideBlocks,
            Map<BlockCoordinates, WrappedBlockData> updates, int minx,
            int maxx, int miny, int maxy, int z, int excminx, int excmaxx,
            int excminy, int excmaxy, int excminz, int excmaxz) {
        for (int y = miny; y < maxy; ++y) {
            for (int x = minx; x < maxx; ++x) {
                if (excminx <= x && x < excmaxx && excminy <= y && y < excmaxy
                        && excminz <= z && z < excmaxz) {
                    continue;
                }
                BlockCoordinates coords = new BlockCoordinates(x, y, z);
                newGuideBlocks.add(coords);
                if (!guideBlocks.contains(coords)) {
                    updates.put(coords, guideMaterials[(x ^ y ^ z) & 1]);
                }
            }
        }
    }

    private void setGuide(BoundingBox newGuide) {
        Location playerLocation = player.getLocation();
        int playerX = playerLocation.getBlockX();
        int playerY = playerLocation.getBlockY();
        int playerZ = playerLocation.getBlockZ();
        int maxDistance = 16 * 4;
        int near = 1;
        int excminx = playerX - near;
        int excmaxx = playerX + near + 1;
        int excminy = playerY - near;
        int excmaxy = playerY + near + 3;
        int excminz = playerZ - near;
        int excmaxz = playerZ + near + 1;
        int nminx = newGuide.getMinX();
        int nmaxx = newGuide.getMaxX();
        int nminy = newGuide.getMinY();
        int nmaxy = newGuide.getMaxY();
        int nminz = newGuide.getMinZ();
        int nmaxz = newGuide.getMaxZ();

        Map<BlockCoordinates, WrappedBlockData> updates = new TreeMap<>();
        Set<BlockCoordinates> newGuideBlocks = new HashSet<>();

        if (nminx != nmaxx) {
            int distx = Math.abs(nminx - playerX);
            if (distx < maxDistance) {
                int minz = Math.max(nminz, playerZ - (maxDistance - distx));
                int maxz = Math.min(nmaxz, playerZ + (maxDistance - distx));
                if (minz < maxz) {
                    setXGuide(newGuideBlocks, updates, nminx, nminy, nmaxy,
                            minz, maxz, excminx, excmaxx, excminy, excmaxy,
                            excminz, excmaxz);
                }
            }
        }

        if (nminx != nmaxx - 1) {
            int distx = Math.abs(nmaxx - 1 - playerX);
            if (distx < maxDistance) {
                int minz = Math.max(nminz, playerZ - (maxDistance - distx));
                int maxz = Math.min(nmaxz, playerZ + (maxDistance - distx));
                if (minz < maxz) {
                    setXGuide(newGuideBlocks, updates, nmaxx - 1, nminy, nmaxy,
                            minz, maxz, excminx, excmaxx, excminy, excmaxy,
                            excminz, excmaxz);
                }
            }
        }

        if (nminy != nmaxy) {
            setYGuide(newGuideBlocks, updates, nminx, nmaxx, nminy, nminz,
                    nmaxz, playerX, playerZ, maxDistance, excminx, excmaxx,
                    excminy, excmaxy, excminz, excmaxz);
        }

        if (nminy != nmaxy - 1) {
            setYGuide(newGuideBlocks, updates, nminx, nmaxx, nmaxy - 1, nminz,
                    nmaxz, playerX, playerZ, maxDistance, excminx, excmaxx,
                    excminy, excmaxy, excminz, excmaxz);
        }

        if (nminz != nmaxz) {
            int distz = Math.abs(nminz - playerZ);
            if (distz < maxDistance) {
                int minx = Math.max(nminx, playerX - (maxDistance - distz));
                int maxx = Math.min(nmaxx, playerX + (maxDistance - distz));
                if (minx < maxx) {
                    setZGuide(newGuideBlocks, updates, minx, maxx, nminy,
                            nmaxy, nminz, excminx, excmaxx, excminy, excmaxy,
                            excminz, excmaxz);
                }
            }
        }

        if (nminz != nmaxz - 1) {
            int distz = Math.abs(nmaxz - 1 - playerZ);
            if (distz < maxDistance) {
                int minx = Math.max(nminx, playerX - (maxDistance - distz));
                int maxx = Math.min(nmaxx, playerX + (maxDistance - distz));
                if (minx < maxx) {
                    setZGuide(newGuideBlocks, updates, minx, maxx, nminy,
                            nmaxy, nmaxz - 1, excminx, excmaxx, excminy,
                            excmaxy, excminz, excmaxz);
                }
            }
        }

        eraseCurrentGuideBlocks(newGuideBlocks, updates);

        guideBlocks = newGuideBlocks;

        Iterator<Map.Entry<BlockCoordinates, WrappedBlockData>> iter = updates
                .entrySet().iterator();
        List<MultiBlockChangeInfo> infoList = new ArrayList<>();
        ChunkCoordIntPair chunkCoords = null;
        int chunkX = Integer.MIN_VALUE;
        int chunkZ = Integer.MIN_VALUE;
        while (iter.hasNext()) {
            Map.Entry<BlockCoordinates, WrappedBlockData> entry = iter.next();
            BlockCoordinates coords = entry.getKey();
            int x = coords.getX();
            int z = coords.getZ();
            if (chunkCoords != null
                    && ((x >> 4) != chunkX || (z >> 4) != chunkZ)) {
                sendBlockUpdate(chunkCoords, infoList);
                infoList.clear();
                chunkCoords = null;
            }
            if (chunkCoords == null) {
                chunkCoords = new ChunkCoordIntPair(x >> 4, z >> 4);
            }
            short location = (short) (((x & 15) << 12) | ((z & 15) << 8)
                    | coords.getY());
            MultiBlockChangeInfo info = new MultiBlockChangeInfo(location,
                    entry.getValue(), chunkCoords);
            infoList.add(info);
        }
        if (chunkCoords != null) {
            sendBlockUpdate(chunkCoords, infoList);
            infoList.clear();
        }
    }

    @SuppressWarnings({ "deprecation" })
    private void eraseCurrentGuideBlocks(Set<BlockCoordinates> newGuideBlocks,
            Map<BlockCoordinates, WrappedBlockData> updates) {
        for (BlockCoordinates coords : guideBlocks) {
            int x = coords.getX();
            int y = coords.getY();
            int z = coords.getZ();
            if (newGuideBlocks.contains(coords)) {
                continue;
            }

            Block block = world.getBlockAt(x, y, z);
            updates.put(coords, WrappedBlockData.createData(block.getType(),
                    block.getData()));
        }
    }

    private void sendBlockUpdate(ChunkCoordIntPair coords,
            List<MultiBlockChangeInfo> infoList) {
        PacketContainer packet = protocolManager
                .createPacket(PacketType.Play.Server.MULTI_BLOCK_CHANGE);
        try {
            packet.getChunkCoordIntPairs().write(0, coords);
            packet.getMultiBlockChangeInfoArrays().write(0, infoList
                    .toArray(new MultiBlockChangeInfo[infoList.size()]));
            protocolManager.sendServerPacket(player, packet);
        } catch (InvocationTargetException e) {
            logger.warning(e, "Cannot send packet " + packet);
        }
    }

}
