package jp.dip.myuminecraft.takenomics;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.bukkit.Chunk;
import org.bukkit.OfflinePlayer;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.block.Sign;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;

import com.Acrobot.Breeze.Utils.PriceUtil;
import com.Acrobot.ChestShop.ChestShop;
import com.Acrobot.ChestShop.Signs.ChestShopSign;
import com.Acrobot.ChestShop.Utils.uName;

public class ChestShopMonitor implements Listener {

    class ScannedSign {
        int x;
        int z;
        int y;
        int world;
        int owner;
        int quantity;
        String material;
        double buyPrice;
        double sellPrice;
    }

    private JavaPlugin        plugin;
    private Logger            logger;
    private Database          database;
    private PlayerMonitor     playerMonitor;
    private Queue<Runnable>   requests = new ArrayDeque<Runnable>();
    private PreparedStatement truncateTemporary;
    private PreparedStatement startTransaction;
    private PreparedStatement deleteObsoleteShops;
    private PreparedStatement insertNewShops;
    private PreparedStatement commit;
    private BukkitRunnable    scheduledTask;

    public ChestShopMonitor(JavaPlugin plugin, Logger logger,
            Database database, PlayerMonitor playerMonitor) {
        this.plugin = plugin;
        this.logger = logger;
        this.database = database;
        this.playerMonitor = playerMonitor;
    }
    
    public boolean enable() {
        Plugin result = plugin.getServer().getPluginManager().getPlugin("ChestShop");
        if (result == null || ! (result instanceof ChestShop)) {
            logger.info("ChestShop is not found.");
            return false;
        }

        try {
            createTables();
            prepareStatements();
            truncateTemporary.executeUpdate();
        } catch (SQLException e) {
            logger.warning("Failed to initialize ChestShop monitor: %s", e.toString());
            for (StackTraceElement t : e.getStackTrace()) {
                logger.warning(t.toString());
            }
            disable();
            return false;
        }

        plugin.getServer().getPluginManager().registerEvents(this, plugin);
        return true;
    }
    
    void createTables() throws SQLException {
        Connection connection = database.getConnection();

        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE chestshops IF NOT EXISTS ("
                + "id SMALLINT UNSIGNED AUTO_INCREMENT NOT NULL,"
                + "x SMALLINT NOT NULL,"
                + "z SMALLINT NOT NULL,"
                + "y SMALLINT NOT NULL,"
                + "world SMALLINT UNSIGNED NOT NULL,"
                + "owner INT UNSIGNED,"
                + "quantity SMALLINT UNSIGNED NOT NULL,"
                + "material VARCHAR(16) CHARACTER SET ascii NOT NULL,"
                + "buy_price DOUBLE,"
                + "sell_price DOUBLE,"
                + "PRIMARY KEY (id),"
                + "UNIQUE (x,z,y,world),"
                + "INDEX (owner),"
                + "FOREIGN KEY (world) REFERENCES worlds (id) ON DELETE CASCADE,"
                + "FOREIGN KEY (owner) REFERENCES players (id) ON DELETE CASCADE,"
                + ")");
        statement.execute("CREATE TABLE chestshop_transactions IF NOT EXISTS ("
                + "id INT UNSIGNED AUTO_INCREMENT NOT NULL,"
                + "timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,"
                + "shop SMALLINT NOT NULL,"
                + "player INT UNSIGNED NOT NULL,"
                + "quantity SMALLINT NOT NULL,"
                + "PRIMARY KEY (id),"
                + "FOREIGN KEY (shop) REFERENCES chestshop (id) ON DELETE CASCADE,"
                + "FOREIGN KEY (player) REFERENCES players (id) ON DELETE CASCADE,"
                + ")");
        statement.execute("CREATE TEMPORARY TABLE scanned_chestshops IF NOT EXISTS ("
                + "x SMALLINT NOT NULL,"
                + "z SMALLINT NOT NULL,"
                + "y SMALLINT NOT NULL,"
                + "world SMALLINT UNSIGNED NOT NULL,"
                + "owner INT UNSIGNED,"
                + "quantity SMALLINT UNSIGNED NOT NULL,"
                + "material VARCHAR(16) CHARACTER SET ascii NOT NULL,"
                + "buy_price DOUBLE,"
                + "sell_price DOUBLE,"
                + "UNIQUE (x,z,y,world),"
                + ")");
    }
    
    void prepareStatements() throws SQLException {
        Connection connection = database.getConnection();

        truncateTemporary = connection.prepareStatement("TRUNCATE scanned_chestshops");
        startTransaction = connection.prepareStatement("START TRANSACTION");
        deleteObsoleteShops = connection.prepareStatement("DELETE FROM chestshops"
                + "USING chestshops LEFT JOIN scanned_chestshops"
                + "ON chestshops.x = scanned_chestshops.x"
                + "AND chestshops.z = scanned_chestshops.z"
                + "AND chestshops.y = scanned_chestshops.y"
                + "AND chestshops.world = scanned_chestshops.world"
                + "WHERE ? <= chestshops.x AND chestshops.x < ?"
                + "AND ? <= chestshops.z AND chestshops.z < ?"
                + "AND (scanned_chestshops.x IS NULL"
                + "OR ! chestshop.owner <=> scanned_chestshops.owner"
                + "OR chestshop.quantity != scanned_chestshops.quantity"
                + "OR chestshops.material != scanned_chestshops.material"
                + "OR chestshops.buy_price != scanned_chestshops.buy_price"
                + "OR chestshops.sell_price != scanned_chestshops.sell_price)");
        insertNewShops = connection.prepareStatement("INSERT INTO chestshops"
                + "SELECT NULL,"
                + "scanned_chestshops.x,"
                + "scanned_chestshops.z,"
                + "scanned_chestshops.y,"
                + "scanned_chestshops.world,"
                + "scanned_chestshops.owner,"
                + "scanned_chestshops.quantity,"
                + "scanned_chestshops.material,"
                + "scanned_chestshops.buy_price,"
                + "scanned_chestshops.sell_price"
                + "FROM scanned_chestshops LEFT JOIN chestshops"
                + "ON chestshops.x = scanned_chestshops.x"
                + "AND chestshops.z = scanned_chestshops.z"
                + "AND chestshops.y = scanned_chestshops.y"
                + "AND chestshops.world = scanned_chestshops.world"
                + "WHERE chestshops.id IS NULL");
        commit = connection.prepareStatement("COMMIT");
    }

    public void disable() {
        try {
            if (startTransaction != null) {
                startTransaction.close();
                startTransaction = null;
            }
            if (truncateTemporary != null) {
                truncateTemporary.close();
                truncateTemporary = null;
            }
            if (deleteObsoleteShops != null) {
                deleteObsoleteShops.close();
                deleteObsoleteShops = null;
            }
            if (insertNewShops != null) {
                insertNewShops.close();
                insertNewShops = null;
            }
            if (commit != null) {
                commit.close();
                commit = null;
            }
            if (deleteObsoleteShops != null) {
                deleteObsoleteShops.close();
                deleteObsoleteShops = null;
            }
        } catch (SQLException e1) {
        }
    }
    
    @EventHandler
    public void onSignScanEvent(SignScanEvent event) {
        try {
            Chunk chunk = event.getChunk();
            World world = chunk.getWorld();
            int worldId = playerMonitor.getWorldId(world);
            final int chunkX = chunk.getX();
            final int chunkZ = chunk.getZ();
            final List<ScannedSign> signs = new ArrayList<ScannedSign>();
            for (Block block : event.getSigns()) {
                if (! ChestShopSign.isValid(block)) {
                    continue;
                }

                Sign sign = (Sign)block.getState();

                String name = uName.getName(sign.getLine(ChestShopSign.NAME_LINE));
                OfflinePlayer owner;
                if (ChestShopSign.isAdminShop(name)) {
                    owner = null;
                } else {
                    owner = plugin.getServer().getOfflinePlayer(name);
                    if (owner == null) {
                        logger.warning("Ignored a ChestShop at %s:%d,%d,%d"
                                + " owned by an unknown user '%s'.",
                                world.getName(), block.getX(),
                                block.getY(), block.getZ(), name);
                        continue;
                    }
                }
                String prices = sign.getLine(ChestShopSign.PRICE_LINE);

                ScannedSign scannedSign = new ScannedSign();
                
                scannedSign.x = block.getX();
                scannedSign.z = block.getZ();
                scannedSign.y = block.getY();
                scannedSign.world = worldId;
                scannedSign.owner = owner == null ? 0 : playerMonitor.getPlayerId(owner);
                scannedSign.quantity = Integer.parseInt(sign.getLine(ChestShopSign.QUANTITY_LINE));
                scannedSign.material = sign.getLine(ChestShopSign.ITEM_LINE);
                scannedSign.buyPrice = PriceUtil.getBuyPrice(prices);
                scannedSign.sellPrice = PriceUtil.getSellPrice(prices);

                signs.add(scannedSign);
            }
            
            if (! signs.isEmpty()) {
                runAsynchronously(new Runnable() {
                    public void run() {
                        syncSigns(chunkX, chunkZ, signs);
                    }
                });
            }

        } catch (SQLException e) {
            logger.warning("Failed to update scanned signs: %s", e.toString());
        }
    }
    
    public synchronized void runAsynchronously(Runnable runnable) {
        requests.add(runnable);
        if (scheduledTask == null) {
            new BukkitRunnable() {
                public void run() {
                    processAsyncTasks();
                }
            }.runTaskAsynchronously(plugin);
        }
    }
    
    void processAsyncTasks() {
        Runnable task;
        synchronized (this) {
            assert ! requests.isEmpty();
            task = requests.peek();
        }

        for (;;) {
            task.run();

            synchronized (this) {
                requests.poll();
                if (requests.isEmpty()) {
                    scheduledTask = null;
                    return;
                }
                task = requests.peek();
            }            
        }
    }

    void syncSigns(int chunkX, int chunkZ, List<ScannedSign> scannedSigns) {
        StringBuilder insert =
                new StringBuilder("INSERT INTO scanned_chestshops VALUES ");
        boolean first = true;
        for (ScannedSign sign : scannedSigns) {
            if (! first) {
                insert.append(",");
            } else {
                first = false;
            }

            insert.append(String.format("(%d,%d,%d,%d,%d,%d,%d",
                    sign.x, sign.z, sign.y, sign.world,
                    sign.owner, sign.quantity, sign.material));
            appendPrice(insert, sign.buyPrice);
            appendPrice(insert, sign.sellPrice);
            insert.append(")");
        }

        Connection connection = database.getConnection();
        try (Statement statement = connection.createStatement()) {
            startTransaction.execute();
            statement.executeUpdate(insert.toString());
            deleteObsoleteShops.setInt(1, chunkX * Constants.chunkXSize);
            deleteObsoleteShops.setInt(2, (chunkX + 1) * Constants.chunkXSize);
            deleteObsoleteShops.setInt(3, chunkZ * Constants.chunkZSize);
            deleteObsoleteShops.setInt(4, (chunkZ + 1) * Constants.chunkZSize);
            deleteObsoleteShops.executeUpdate();
            insertNewShops.executeUpdate();
            truncateTemporary.executeUpdate();
            commit.executeUpdate();
        } catch (SQLException e) {
            logger.warning("Failed to sync chestshops table: %s", e.toString());
        }
    }
    
    void appendPrice(StringBuilder target, double price) {
        if (price < 0.0) {
            target.append(",NULL");
        } else {
            target.append(String.format(",%f", price));
        }
    }            
    
}
