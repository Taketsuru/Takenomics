package jp.dip.myuminecraft.takenomics;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.bukkit.Chunk;
import org.bukkit.OfflinePlayer;
import org.bukkit.block.Sign;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;

import com.Acrobot.Breeze.Utils.PriceUtil;
import com.Acrobot.ChestShop.ChestShop;
import com.Acrobot.ChestShop.Events.ShopCreatedEvent;
import com.Acrobot.ChestShop.Events.ShopDestroyedEvent;
import com.Acrobot.ChestShop.Events.TransactionEvent;
import com.Acrobot.ChestShop.Events.TransactionEvent.TransactionType;
import com.Acrobot.ChestShop.Signs.ChestShopSign;
import com.Acrobot.ChestShop.Utils.uName;

public class ChestShopMonitor implements Listener {

    class ChestshopsRow {
        int x;
        int z;
        int y;
        int world;
        int owner;
        int quantity;
        String material;
        double buyPrice;
        double sellPrice;
        
        ChestshopsRow(OfflinePlayer player, Sign sign, String[] lines)
                throws UnknownPlayerException, SQLException {
            x = sign.getX();
            z = sign.getZ();
            y = sign.getY();
            world = playerMonitor.getWorldId(sign.getWorld());

            String nameOnSign = lines[ChestShopSign.NAME_LINE];
            logger.info("nameOnSign=" + nameOnSign);
            String ownerName = nameOnSign.isEmpty()
                    ? player.getName()
                    : uName.getName(nameOnSign);

            OfflinePlayer ownerPlayer;
            if (ChestShopSign.isAdminShop(ownerName)) {
                ownerPlayer = null;
            } else {
                ownerPlayer = plugin.getServer().getOfflinePlayer(ownerName);
                if (ownerPlayer == null) {
                    throw new UnknownPlayerException
                    (String.format("ChestShop at %s:%d,%d,%d"
                            + " is owned by an unknown user '%s'.",
                            sign.getWorld().getName(), sign.getX(),
                            sign.getY(), sign.getZ(), ownerName));
                }
            }
            owner = ownerPlayer == null ? 0 : playerMonitor.getPlayerId(ownerPlayer);
            
            quantity = Integer.parseInt(lines[ChestShopSign.QUANTITY_LINE]);
            material = lines[ChestShopSign.ITEM_LINE];

            String prices = lines[ChestShopSign.PRICE_LINE];
            buyPrice = PriceUtil.getBuyPrice(prices);
            sellPrice = PriceUtil.getSellPrice(prices);
        }
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
    private PreparedStatement insertTransaction;
    private PreparedStatement selectIdFromShops;
    private PreparedStatement insertShop;
    private PreparedStatement insertShopReturnKey;
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
        statement.execute("CREATE TABLE IF NOT EXISTS chestshops ("
                + "id SMALLINT UNSIGNED AUTO_INCREMENT NOT NULL,"
                + "x SMALLINT NOT NULL,"
                + "z SMALLINT NOT NULL,"
                + "y SMALLINT NOT NULL,"
                + "world TINYINT UNSIGNED NOT NULL,"
                + "owner MEDIUMINT UNSIGNED,"
                + "quantity SMALLINT UNSIGNED NOT NULL,"
                + "material VARCHAR(16) CHARACTER SET ascii NOT NULL,"
                + "buy_price DOUBLE,"
                + "sell_price DOUBLE,"
                + "PRIMARY KEY (id),"
                + "UNIQUE (x,z,y,world),"
                + "INDEX (owner),"
                + "FOREIGN KEY (world) REFERENCES worlds (id) ON DELETE CASCADE,"
                + "FOREIGN KEY (owner) REFERENCES players (id) ON DELETE CASCADE"
                + ")");
        statement.execute("CREATE TABLE IF NOT EXISTS chestshop_transactions ("
                + "id INT UNSIGNED AUTO_INCREMENT NOT NULL,"
                + "timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,"
                + "shop SMALLINT UNSIGNED NOT NULL,"
                + "player MEDIUMINT UNSIGNED NOT NULL,"
                + "type ENUM('buy', 'sell') NOT NULL,"
                + "quantity SMALLINT NOT NULL,"
                + "PRIMARY KEY (id),"
                + "FOREIGN KEY (shop) REFERENCES chestshops (id) ON DELETE CASCADE,"
                + "FOREIGN KEY (player) REFERENCES players (id) ON DELETE CASCADE"
                + ")");
        statement.execute("CREATE TEMPORARY TABLE IF NOT EXISTS scanned_chestshops ("
                + "x SMALLINT NOT NULL,"
                + "z SMALLINT NOT NULL,"
                + "y SMALLINT NOT NULL,"
                + "world SMALLINT UNSIGNED NOT NULL,"
                + "owner INT UNSIGNED,"
                + "quantity SMALLINT UNSIGNED NOT NULL,"
                + "material VARCHAR(16) CHARACTER SET ascii NOT NULL,"
                + "buy_price DOUBLE,"
                + "sell_price DOUBLE,"
                + "UNIQUE (x,z,y,world)"
                + ") ENGINE=MEMORY");
    }
    
    void prepareStatements() throws SQLException {
        Connection connection = database.getConnection();

        truncateTemporary = connection.prepareStatement("TRUNCATE scanned_chestshops");
        startTransaction = connection.prepareStatement("START TRANSACTION");
        deleteObsoleteShops = connection.prepareStatement("DELETE FROM chestshops "
                + "USING chestshops LEFT JOIN scanned_chestshops "
                + "ON chestshops.x = scanned_chestshops.x "
                + "AND chestshops.z = scanned_chestshops.z "
                + "AND chestshops.y = scanned_chestshops.y "
                + "AND chestshops.world = scanned_chestshops.world "
                + "WHERE ? <= chestshops.x AND chestshops.x < ? "
                + "AND ? <= chestshops.z AND chestshops.z < ? "
                + "AND (scanned_chestshops.x IS NULL "
                + "OR ! chestshops.owner <=> scanned_chestshops.owner "
                + "OR chestshops.quantity != scanned_chestshops.quantity "
                + "OR chestshops.material != scanned_chestshops.material "
                + "OR chestshops.buy_price != scanned_chestshops.buy_price "
                + "OR chestshops.sell_price != scanned_chestshops.sell_price)");
        insertNewShops = connection.prepareStatement("INSERT INTO chestshops "
                + "SELECT NULL,"
                + "scanned_chestshops.x,"
                + "scanned_chestshops.z,"
                + "scanned_chestshops.y,"
                + "scanned_chestshops.world,"
                + "scanned_chestshops.owner,"
                + "scanned_chestshops.quantity,"
                + "scanned_chestshops.material,"
                + "scanned_chestshops.buy_price,"
                + "scanned_chestshops.sell_price "
                + "FROM scanned_chestshops LEFT JOIN chestshops "
                + "ON chestshops.x = scanned_chestshops.x "
                + "AND chestshops.z = scanned_chestshops.z "
                + "AND chestshops.y = scanned_chestshops.y "
                + "AND chestshops.world = scanned_chestshops.world "
                + "WHERE chestshops.id IS NULL");
        commit = connection.prepareStatement("COMMIT");
        insertShop = connection.prepareStatement
                ("INSERT INTO chestshops VALUES (NULL,?,?,?,?,?,?,?,?,?)");
        insertShopReturnKey = connection.prepareStatement
                ("INSERT INTO chestshops VALUES (NULL,?,?,?,?,?,?,?,?,?)",
                  Statement.RETURN_GENERATED_KEYS);
        selectIdFromShops = connection.prepareStatement
                ("SELECT id FROM chestshops WHERE x=? AND z=? AND y=? AND world=?");
        insertTransaction = connection.prepareStatement
                ("INSERT INTO chestshop_transactions VALUES (NULL,NULL,?,?,?,?)");
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
            if (insertShop != null) {
                insertShop.close();
                insertShop = null;
            }
            if (insertShopReturnKey != null) {
                insertShopReturnKey.close();
                insertShopReturnKey = null;
            }
            if (selectIdFromShops != null) {
                selectIdFromShops.close();
                selectIdFromShops = null;
            }
            if (insertTransaction != null) {
                insertTransaction.close();
                insertTransaction = null;
            }
        } catch (SQLException e1) {
        }
    }
    
    @EventHandler
    public void onSignScanEvent(SignScanEvent event) {
        try {
            Chunk chunk = event.getChunk();
            final int chunkX = chunk.getX();
            final int chunkZ = chunk.getZ();
            final List<ChestshopsRow> signs = new ArrayList<ChestshopsRow>();
            for (Sign sign : event.getSigns()) {
                if (ChestShopSign.isValid(sign)) {
                    signs.add(new ChestshopsRow(null, sign, sign.getLines()));
                }
            }

            if (! signs.isEmpty()) {
                runAsynchronously(new Runnable() {
                    public void run() {
                        syncSigns(chunkX, chunkZ, signs);
                    }
                });
            }
        } catch (Exception e) {
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
    
    void syncSigns(int chunkX, int chunkZ, List<ChestshopsRow> scannedSigns) {
        StringBuilder insert =
                new StringBuilder("INSERT INTO scanned_chestshops VALUES ");
        boolean first = true;
        for (ChestshopsRow row : scannedSigns) {
            if (! first) {
                insert.append(",");
            } else {
                first = false;
            }
            
            insert.append(String.format("(%d,%d,%d,%d",
                    row.x, row.z, row.y, row.world));
            insert.append(row.owner != 0
                    ? String.format(",%d", row.owner)
                    : ",NULL");
            insert.append(String.format(",%d,'%s'",
                    row.quantity, Database.escapeSingleQuotes(row.material)));
            appendPrice(insert, row.buyPrice);
            appendPrice(insert, row.sellPrice);
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

    @EventHandler
    void onShopCreatedEvent(ShopCreatedEvent event) {
        try {
            final ChestshopsRow row = new ChestshopsRow
                    (event.getPlayer(), event.getSign(), event.getSignLines());
            runAsynchronously(new Runnable() {
                public void run() {
                    try {
                        insertIntoShops(row);
                    } catch (SQLException e) {
                        logger.warning(e, "Failed to enter shop record.");
                    }
                }
            });
        } catch (Exception e) {
            logger.warning(e, "Failed to enter shop record.");
        }
    }

    void setInsertIntoShopsParams(ChestshopsRow row, PreparedStatement stmt) throws SQLException {
        stmt.setInt(1, row.x);
        stmt.setInt(2, row.z);
        stmt.setInt(3, row.y);
        stmt.setInt(4, row.world);
        stmt.setInt(5, row.owner);
        stmt.setInt(6, row.quantity);
        stmt.setString(7, row.material);
        if (row.buyPrice != -1.0) {
            stmt.setDouble(8, row.buyPrice);
        } else {
            stmt.setNull(8, Types.DOUBLE);
        }
        if (row.sellPrice != -1.0) {
            stmt.setDouble(9, row.sellPrice);
        } else {
            stmt.setNull(9, Types.DOUBLE);
        }
    }

    void insertIntoShops(ChestshopsRow row) throws SQLException {
        setInsertIntoShopsParams(row, insertShop);
        insertShop.executeUpdate();
    }

    int insertIntoShopsReturnKey(ChestshopsRow row) throws SQLException {
        setInsertIntoShopsParams(row, insertShopReturnKey);
        insertShopReturnKey.executeUpdate();
        try (ResultSet rs = insertShopReturnKey.getGeneratedKeys()) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @EventHandler
    void onShopDestroyedEvent(ShopDestroyedEvent event) {
        try {
            final ChestshopsRow row = new ChestshopsRow
                    (null, event.getSign(), event.getSign().getLines());
            runAsynchronously(new Runnable() {
                public void run() {
                    try {
                        deleteFromShops(row);
                    } catch (SQLException e) {
                        logger.warning(e, "Failed to delete shop record.");
                    }
                }
            });
        } catch (Exception e) {
            logger.warning(e, "Failed to delete shop record.");
        }
    }

    void deleteFromShops(ChestshopsRow row) throws SQLException {
        try (Statement statement = database.getConnection().createStatement()) {
            String stmt = String.format("DELETE FROM chestshops WHERE "
                    + "x = %d "
                    + "AND z = %d "
                    + "AND y = %d "
                    + "AND world = %d",
                    row.x, row.z, row.y, row.world);
            statement.executeUpdate(stmt);
        }
    }

    @EventHandler
    void onTransactionEvent(TransactionEvent event) {
        try {
            final ChestshopsRow row =
                    new ChestshopsRow(null, event.getSign(), event.getSign().getLines());
            final int amount = event.getStock()[0].getAmount();
            final TransactionType type = event.getTransactionType();
            final int playerId = playerMonitor.getPlayerId(event.getClient());
            runAsynchronously(new Runnable() {
                public void run() {
                    try {
                        insertTransactionRecord(row, playerId, type, amount);
                    } catch (SQLException e) {
                        logger.warning(e, "Failed to insert transaction record.");
                    }
                }
            });
        } catch (Exception e) {
            logger.warning(e, "Failed to delete shop record.");
        }
    }

    void insertTransactionRecord(ChestshopsRow row, int playerId,
            TransactionType type, int amount) throws SQLException {

        try (Statement statement = database.getConnection().createStatement()) {
            startTransaction.executeUpdate();

            selectIdFromShops.setInt(1, row.x);
            selectIdFromShops.setInt(2, row.z);
            selectIdFromShops.setInt(3, row.y);
            selectIdFromShops.setInt(4, row.world);
            
            int shopId;
            try (ResultSet resultSet = selectIdFromShops.executeQuery()) {            
                shopId = resultSet.next()
                        ? resultSet.getInt(1)
                        : insertIntoShopsReturnKey(row);
            }

            insertTransaction.setInt(1, shopId);
            insertTransaction.setInt(2, playerId);
            insertTransaction.setString(3, type.toString().toLowerCase());
            insertTransaction.setInt(4, amount);
            insertTransaction.executeUpdate();

            commit.executeUpdate();
        }
        
    }
  
}
