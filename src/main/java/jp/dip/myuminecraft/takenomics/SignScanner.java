package jp.dip.myuminecraft.takenomics;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bukkit.Chunk;
import org.bukkit.Material;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.event.Event;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;

public class SignScanner {

    class Request {
        CommandSender sender;
        World         world;

        Request(CommandSender sender, World world) {
            this.sender = sender;
            this.world = world;
        }
    }

    enum ChunkState {
        notReady,
        fetching,
        loading,
        loaded
    }

    static final int      interval           = 1;
    static final int      sizeOfInt          = 4;
    static final int      log2SectorSize     = 12;
    static final int      regionXSize        = 32;
    static final int      regionZSize        = 32;
    static final int      chunksPerRegion    = regionXSize * regionZSize;
    static final int      chunkLocationsSize = sizeOfInt * chunksPerRegion;
    static final int      maxChunkSize       = 1024 * 1024;
    static final Pattern  regionFilePattern  = Pattern.compile("^r\\.(-?[0-9]+)\\.(-?[0-9]+)\\.mca$");
    JavaPlugin            plugin;
    Logger                logger;
    Messages              messages;
    CommandDispatcher     commandDispatcher;
    CommandDispatcher     scanSignsDispatcher;
    BukkitRunnable        scheduledTask;
    Queue<Request>        requests           = new LinkedList<Request>();
    List<Block>           signBlocks         = new ArrayList<Block>();
    ByteBuffer            prefetchBuffer     = ByteBuffer.allocateDirect(maxChunkSize);
    ByteBuffer            chunkLocations     = ByteBuffer.allocateDirect(chunkLocationsSize);
    DirectoryStream<Path> directoryStream;
    Iterator<Path>        regionFiles;
    Path                  currentFile;
    int                   regionX;
    int                   regionZ;
    int                   nextFile;
    int                   nextChunk;
    ChunkState            chunkState;
    boolean               running;

    public SignScanner(JavaPlugin plugin, Logger logger,
            Messages messages, CommandDispatcher commandDispatcher) {
        this.plugin = plugin;
        this.logger = logger;
        this.messages = messages;
        this.commandDispatcher = commandDispatcher;
    }

    public void enable() {
        addCommands();
    }

    public void disable() {
        if (scheduledTask != null) {
            scheduledTask.cancel();
            scheduledTask = null;
        }
        requests.clear();
        directoryStream = null;
        regionFiles = null;
        currentFile = null;
        regionX = 0;
        regionZ = 0;
        nextFile = 0;
        chunkState = ChunkState.notReady;
        running = false;
    }

    public void scanSigns(CommandSender sender, World world) {
        requests.add(new Request(sender, world));
        if (! running) {
            schedule();
        }
    }

    public void scanAllSigns(CommandSender sender) {
        for (World world: plugin.getServer().getWorlds()) {
            scanSigns(sender, world);
        }
    }

    void schedule() {
        running = true;
        scheduledTask = new BukkitRunnable() {
            public void run() {
                processRequests();
            }
        };
        scheduledTask.runTaskLater(plugin, interval);
    }

    void processRequests() {
        long stime = System.nanoTime();
        long etime = stime + 2 * 1000 * 1000; // 2ms
        majorLoop: while (System.nanoTime() < etime) {

            if (directoryStream == null) {
                logger.info("processRequests: loading regions folder");
                do {
                    Request head = requests.peek();
                    if (head == null) {
                        logger.info("processRequests: stop");
                        running = false;
                        scheduledTask = null;
                        return;
                    }

                    Path regionFolder = head.world.getWorldFolder().toPath()
                            .resolve("region");
                    try {
                        directoryStream = Files
                                .newDirectoryStream(regionFolder);
                    } catch (IOException e) {
                        messages.send(head.sender,
                                "signScannerFailedToReadRegionFiles",
                                head.world.getName(), e.getMessage());
                        requests.poll();
                    }
                } while (directoryStream == null);

                regionFiles = directoryStream.iterator();
                currentFile = null;
            }

            while (currentFile == null) {
                logger.info("processRequests: loading region file");
                if (!regionFiles.hasNext()) {
                    try {
                        directoryStream.close();
                    } catch (IOException e) {
                    }
                    directoryStream = null;
                    regionFiles = null;
                    Request head = requests.peek();
                    messages.send(head.sender,
                            "signScannerFinishedScanningWorld",
                            head.world.getName());
                    requests.poll();
                    continue majorLoop;
                }

                currentFile = regionFiles.next();
                Matcher matcher = regionFilePattern.matcher(currentFile
                        .getFileName().toString());
                if (!matcher.matches()) {
                    currentFile = null;
                    continue;
                }

                logger.info("processRequests: file %s", currentFile.toString());
                try (SeekableByteChannel channel = Files.newByteChannel(
                        currentFile, StandardOpenOption.READ)) {
                    chunkLocations.clear();
                    while (0 < chunkLocations.remaining()) {
                        if (channel.read(chunkLocations) == -1) {
                            throw new IOException("file is corrupted.");
                        }
                    }
                    chunkLocations.flip();
                } catch (IOException e) {
                    messages.send(requests.peek().sender,
                            "signScannerFailedToRead", currentFile.toString(),
                            e.getMessage());
                    currentFile = null;
                    continue;
                }

                nextChunk = 0;
                chunkState = ChunkState.notReady;
                regionX = Integer.parseInt(matcher.group(1)) * 32;
                regionZ = Integer.parseInt(matcher.group(2)) * 32;
                logger.info("processRequests: region x = %d, region z = %d", regionX, regionZ);
            }

            Request request = requests.peek();
            World world = request.world;

            if (chunksPerRegion <= nextChunk) {
                currentFile = null;
                continue;
            }

            if (chunkLocations.getInt(nextChunk * sizeOfInt) != 0) {

                int chunkX = regionX + nextChunk % regionXSize;
                int chunkZ = regionZ + nextChunk / regionXSize;

                switch (chunkState) {

                case notReady:
                    if (world.isChunkLoaded(chunkX, chunkZ)) {
                        chunkState = ChunkState.loaded;
                        break;
                    }

                    chunkState = ChunkState.fetching;
                    new BukkitRunnable() {
                        public void run() {
                            fetch();
                        }
                    }.runTaskAsynchronously(plugin);
                    break majorLoop;

                case fetching:
                    break majorLoop;

                case loading:
                    world.loadChunk(chunkX, chunkZ, false);
                    chunkState = ChunkState.loaded;
                    continue majorLoop;

                case loaded:
                    break;
                }

                Chunk chunk = world.getChunkAt(chunkX, chunkZ);
                if (chunk.getBlock(0, 0, 0).getType() != Material.BEDROCK) {
                    logger.warning("not bedrock?!");
                }
                    
                scanChunk(chunk);
                world.unloadChunkRequest(chunkX, chunkZ);
            }

            ++nextChunk;
            chunkState = ChunkState.notReady;
            if (nextChunk % 32 == 0) {
                logger.info("processed %d chunks", nextChunk);
            }
        }

        schedule();
    }

    void scanChunk(Chunk chunk) {
        signBlocks.clear();

        int chunkXSize = 16;
        int chunkYSize = 256;
        int chunkZSize = 16;

        for (int y = 0; y < chunkYSize; ++y) {
            for (int z = 0; z < chunkZSize; ++z) {
                for (int x = 0; x < chunkXSize; ++x) {
                    Block block = chunk.getBlock(x, y, z);
                    switch (block.getType()) {
                    case SIGN_POST:
                    case WALL_SIGN:
                        logger.info("scanChunk: found: %d,%d,%d", x, y, z);
                        signBlocks.add(block);
                        break;

                    default:
                        break;
                    }
                }
            }
        }

        if (!signBlocks.isEmpty()) {
            logger.info("scanChunk: notifying");
            Event event = new SignScanEvent(chunk, signBlocks);
            plugin.getServer().getPluginManager().callEvent(event);
        }
    }

    void fetch() {
        int loc = chunkLocations.getInt(nextChunk * sizeOfInt);
        assert loc != 0;
        
        int offset = (loc >> 8) << log2SectorSize;
        int count = (loc & 0xff) << log2SectorSize;

        try (SeekableByteChannel channel = Files.newByteChannel(
                currentFile, StandardOpenOption.READ)) {
            channel.position(offset);
            prefetchBuffer.clear();
            prefetchBuffer.limit(count);
            while (prefetchBuffer.hasRemaining()) {
                if (channel.read(prefetchBuffer) == -1) {
                    throw new IOException("file is corrupted.");
                }
            }
        } catch (IOException e) {
            logger.warning("%s", e.toString());
            for (StackTraceElement elm : e.getStackTrace()) {
                logger.warning("%s", elm.toString());
            }
        }

        chunkState = ChunkState.loading;
    }

    void addCommands() {
        scanSignsDispatcher = new CommandDispatcher(commandDispatcher, "scansigns");
        
        scanSignsDispatcher.addCommand("world", new CommandExecutor() {
            public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
                return onScansignsWorldCommand(sender, cmd, label, args);
            }
        });                

        scanSignsDispatcher.addCommand("all", new CommandExecutor() {
            public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
                return onScansignsAllCommand(sender, cmd, label,args);
            }
        });
    }

    boolean onScansignsWorldCommand(CommandSender sender, Command cmd, String label, String[] args) {
        int cmdPos = scanSignsDispatcher.getCommandPosition();

        if (args.length == cmdPos + 1) {
            if (! (sender instanceof Player)) {
                messages.send(sender, "notAConsoleCommand",
                        scanSignsDispatcher.getCommandString(cmd, args));
                return true;
            }
            scanSigns(sender, ((Player)sender).getLocation().getWorld());
            return true;
        }

        if (args.length == cmdPos + 2) {
            String worldName = args[cmdPos + 1];
            World world = sender.getServer().getWorld(worldName);
            if (world == null) {
                messages.send(sender, "worldNotFound", worldName);
                return true;
            }
            scanSigns(sender, world);
            return true;
        }

        return false;
    }

    boolean onScansignsAllCommand(CommandSender sender, Command cmd, String label, String[] args) {
        int cmdPos = scanSignsDispatcher.getCommandPosition();

        if (args.length == cmdPos + 1) {
            scanAllSigns(sender);
            return true;
        }

        return false;
    }

}
