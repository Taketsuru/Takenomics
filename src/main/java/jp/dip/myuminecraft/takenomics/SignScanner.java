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
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.block.Sign;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.event.Event;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;

import jp.dip.myuminecraft.takecore.Logger;
import jp.dip.myuminecraft.takecore.Messages;

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
    static final Pattern  regionFilePattern  = Pattern.compile("^r\\.(-?[0-9]+)\\.(-?[0-9]+)\\.mca$");
    static final Pattern  xzCoordPattern     = Pattern.compile("^\\s*([-+]?[0-9]+),\\s*([-+]?[0-9]+)\\s$");
    JavaPlugin            plugin;
    Logger                logger;
    Messages              messages;
    CommandDispatcher     commandDispatcher;
    CommandDispatcher     scanSignsDispatcher;
    BukkitRunnable        scheduledTask;
    Queue<Request>        requests           = new LinkedList<Request>();
    List<Sign>            signBlocks         = new ArrayList<Sign>();
    ByteBuffer            prefetchBuffer     = ByteBuffer.allocateDirect(Constants.maxChunkSize);
    ByteBuffer            chunkLocations     = ByteBuffer.allocateDirect(Constants.chunkLocationsSize);
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
                do {
                    Request head = requests.peek();
                    if (head == null) {
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
            }

            Request request = requests.peek();
            World world = request.world;

            if (Constants.chunksPerRegion <= nextChunk) {
                currentFile = null;
                continue;
            }

            if (chunkLocations.getInt(nextChunk * Constants.sizeOfInt) != 0) {

                int chunkX = regionX + nextChunk % Constants.regionXSize;
                int chunkZ = regionZ + nextChunk / Constants.regionXSize;

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
                scanChunk(chunk);
                world.unloadChunkRequest(chunkX, chunkZ);
            }

            ++nextChunk;
            chunkState = ChunkState.notReady;
        }

        schedule();
    }

    void scanChunk(Chunk chunk) {
        signBlocks.clear();

        for (int y = 0; y < Constants.chunkYSize; ++y) {
            for (int z = 0; z < Constants.chunkZSize; ++z) {
                for (int x = 0; x < Constants.chunkXSize; ++x) {
                    Block block = chunk.getBlock(x, y, z);
                    switch (block.getType()) {
                    case SIGN_POST:
                    case WALL_SIGN:
                        signBlocks.add((Sign)block.getState());
                        break;

                    default:
                        break;
                    }
                }
            }
        }

        Event event = new SignScanEvent(chunk, signBlocks);
        plugin.getServer().getPluginManager().callEvent(event);
    }

    void fetch() {
        int loc = chunkLocations.getInt(nextChunk * Constants.sizeOfInt);
        assert loc != 0;
        
        int offset = (loc >> 8) << Constants.log2SectorSize;
        int count = (loc & 0xff) << Constants.log2SectorSize;

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
            logger.warning(e, "Failed to prefetch region file.");
        }

        chunkState = ChunkState.loading;
    }

    void addCommands() {
        scanSignsDispatcher = new CommandDispatcher(commandDispatcher, "scansigns");
        
        scanSignsDispatcher.addCommand("chunk", new CommandExecutor() {
            public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
                return onScansignsChunkCommand(sender, cmd, label, args);
            }
        });                

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

    boolean onScansignsChunkCommand(CommandSender sender,
            Command cmd, String label, String[] args) {
        int cmdPos = scanSignsDispatcher.getCommandPosition();

        if (args.length == cmdPos + 1) {
            if (! (sender instanceof Player)) {
                messages.send(sender, "notAConsoleCommand",
                        scanSignsDispatcher.getCommandString(cmd, args));
                return true;
            }
            scanChunk(((Player)sender).getLocation().getChunk());
            return true;
        }

        if (args.length == cmdPos + 3) {
            String worldName = args[cmdPos + 1];
            World world = sender.getServer().getWorld(worldName);
            if (world == null) {
                messages.send(sender, "worldNotFound", worldName);
                return true;
            }

            String coord = args[cmdPos + 2];
            Matcher matcher = xzCoordPattern.matcher(coord);
            if (! matcher.matches()) {
                messages.send(sender, "signScannerXzCoordsAreExpected", coord);
               return true;
            }
            
            int x = Integer.parseInt(matcher.group(1)) / Constants.chunkXSize;
            int z = Integer.parseInt(matcher.group(2)) / Constants.chunkZSize;
            world.loadChunk(x, z, false);
            if (world.isChunkLoaded(x, z)) {
                scanChunk(world.getChunkAt(x, z));
                world.unloadChunkRequest(x, z);
            }
            return true;
        }

        return false;
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
