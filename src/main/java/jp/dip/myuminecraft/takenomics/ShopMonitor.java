package jp.dip.myuminecraft.takenomics;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import jp.dip.myuminecraft.takenomics.models.PlayerTable;
import jp.dip.myuminecraft.takenomics.models.ShopTable;
import jp.dip.myuminecraft.takenomics.models.ShopTable.Shop;
import jp.dip.myuminecraft.takenomics.models.WorldTable;

import org.bukkit.Chunk;
import org.bukkit.Material;
import org.bukkit.Server;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.block.BlockFace;
import org.bukkit.block.BlockState;
import org.bukkit.block.Chest;
import org.bukkit.block.DoubleChest;
import org.bukkit.block.Sign;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.inventory.InventoryCloseEvent;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.InventoryHolder;
import org.bukkit.inventory.ItemStack;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.java.JavaPlugin;

import com.Acrobot.Breeze.Utils.MaterialUtil;
import com.Acrobot.Breeze.Utils.PriceUtil;
import com.Acrobot.ChestShop.ChestShop;
import com.Acrobot.ChestShop.Events.ShopCreatedEvent;
import com.Acrobot.ChestShop.Events.ShopDestroyedEvent;
import com.Acrobot.ChestShop.Events.TransactionEvent;
import com.Acrobot.ChestShop.Events.TransactionEvent.TransactionType;
import com.Acrobot.ChestShop.Signs.ChestShopSign;
import com.Acrobot.ChestShop.UUIDs.NameManager;
import com.Acrobot.ChestShop.Utils.uBlock;

public class ShopMonitor implements Listener, ShopValidator {

    public static final int currentSchemaVersion = 1;
    public static final int maxBatchSize         = 100;
    JavaPlugin              plugin;
    Logger                  logger;
    CommandDispatcher       commandDispatcher;
    Database                database;
    PlayerTable             playerTable;
    WorldTable              worldTable;
    ShopTable               shopTable;
    String                  transactionTableName;
    PreparedStatement       insertTransaction;

    public ShopMonitor(JavaPlugin plugin, final Logger logger, Database database,
            PlayerTable playerTable, WorldTable worldTable, final ShopTable shopTable) {
        this.plugin = plugin;
        this.logger = logger;
        this.database = database;
        this.playerTable = playerTable;
        this.worldTable = worldTable;
        this.shopTable = shopTable;
    }

    public boolean enable() {
        if (database == null || playerTable == null || worldTable == null || shopTable == null) {
            logger.warning("No database connection.  Disabled chestshop monitor.");
            return true;
        }

        Plugin result = plugin.getServer().getPluginManager().getPlugin("ChestShop");
        if (result == null || !(result instanceof ChestShop)) {
            logger.warning("ChestShop is not found.");
            return true;
        }

        String tablePrefix = database.getTablePrefix();
        transactionTableName = tablePrefix + "transactions";

        try {
            createTransactionTableIfNecessary();
            prepareStatements();
        } catch (SQLException e) {
            logger.warning(e, "Failed to initialize ChestShop monitor.");
            disable();
            return true;
        }
        
        shopTable.addValidator(this);

        plugin.getServer().getPluginManager().registerEvents(this, plugin);

        return false;
    }


    Shop newShop(Sign sign, String[] lines) throws UnknownPlayerException,
            SQLException {
        int x = sign.getX();
        int z = sign.getZ();
        int y = sign.getY();
        String world = sign.getWorld().getName();

        UUID owner;
        if (ChestShopSign.isAdminShop(sign)) {
            owner = null;
        } else {
            String nameOnSign = lines[ChestShopSign.NAME_LINE];
            String ownerName = NameManager.getFullUsername(nameOnSign);
            owner = NameManager.getUUID(ownerName);
        }

        int quantity = Integer.parseInt(lines[ChestShopSign.QUANTITY_LINE]);

        String prices = lines[ChestShopSign.PRICE_LINE];
        double buyPrice = PriceUtil.getBuyPrice(prices);
        double sellPrice = PriceUtil.getSellPrice(prices);

        String material = lines[ChestShopSign.ITEM_LINE];

        int stock;
        int purchasableQuantity;

        if (ChestShopSign.isAdminShop(sign)) {
            stock = -1;
            purchasableQuantity = -1;
        } else {
            stock = 0;
            purchasableQuantity = 0;

            Chest chest = uBlock.findConnectedChest(sign);
            if (chest != null) {
                Inventory inventory = chest.getInventory();

                ItemStack item = MaterialUtil.getItem(material);
                if (item != null) {
                    int slotCount = inventory.getSize();
                    for (int i = 0; i < slotCount; ++i) {
                        ItemStack slot = inventory.getItem(i);
                        if (slot == null || slot.getType().equals(Material.AIR)) {
                            purchasableQuantity += item.getMaxStackSize();
                        } else if (slot.isSimilar(item)) {
                            purchasableQuantity += item.getMaxStackSize()
                                    - slot.getAmount();
                            stock += slot.getAmount();
                        }
                    }
                }
            }
        }

        return new Shop(x, z, y, world, owner, quantity, material, buyPrice,
                sellPrice, stock, purchasableQuantity);
    }

    void createTransactionTableIfNecessary() throws SQLException {
        Connection connection = database.getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format("CREATE TABLE IF NOT EXISTS %s ("
                    + "id INT UNSIGNED AUTO_INCREMENT NOT NULL,"
                    + "timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,"
                    + "shop SMALLINT UNSIGNED NOT NULL,"
                    + "player MEDIUMINT UNSIGNED NOT NULL,"
                    + "type ENUM('buy', 'sell') NOT NULL,"
                    + "quantity SMALLINT NOT NULL," + "PRIMARY KEY (id),"
                    + "FOREIGN KEY (shop) REFERENCES %s (id),"
                    + "FOREIGN KEY (player) REFERENCES %s (id)" + ")",
                    transactionTableName, shopTable.getTableName(),
                    playerTable.getTableName()));
        }
    }

    void prepareStatements() throws SQLException {
        Connection connection = database.getConnection();

        insertTransaction = connection.prepareStatement(String.format(
                "INSERT INTO %s VALUES (NULL,NULL,?,?,?,?)",
                transactionTableName));
    }

    public void disable() {
        if (shopTable != null) {
            shopTable.removeValidator(this);
        }

        transactionTableName = null;

        if (insertTransaction != null) {
            try {
                insertTransaction.close();
            } catch (SQLException e) {
            }
            insertTransaction = null;
        }
    }

    @EventHandler
    void onShopCreatedEvent(ShopCreatedEvent event) {
        try {
            final Shop row = newShop(event.getSign(), event.getSignLines());
            database.runAsynchronously(new Runnable() {
                public void run() {
                    try {
                        Connection connection = database.getConnection();
                        connection.setAutoCommit(false);
                        try {
                            shopTable.put(row);
                            connection.commit();
                        } catch (SQLException e) {
                            connection.rollback();
                            throw e;
                        } finally {
                            connection.setAutoCommit(true);
                        }
                    } catch (SQLException e) {
                        logger.warning(e, "Failed to enter a shop record.");
                    }
                }
            });
        } catch (Exception e) {
            logger.warning(e, "Failed to enter shop record.");
        }
    }

    @EventHandler
    void onShopDestroyedEvent(ShopDestroyedEvent event) {
        try {
            final Shop row = newShop(event.getSign(), event.getSign().getLines());
            database.runAsynchronously(new Runnable() {
                public void run() {
                    try {
                        shopTable.remove(row.world, row.x, row.y, row.z);
                    } catch (SQLException e) {
                        logger.warning(e, "Failed to delete shop record.");
                    }
                }
            });
        } catch (Exception e) {
            logger.warning(e, "Failed to delete shop record.");
        }
    }

    @EventHandler
    void onTransactionEvent(TransactionEvent event) {
        try {
            final Shop row = newShop(event.getSign(), event.getSign().getLines());
            final int amount = event.getStock()[0].getAmount();
            final TransactionType type = event.getTransactionType();
            final int playerId = playerTable.getId(event.getClient().getUniqueId());
            database.runAsynchronously(new Runnable() {
                public void run() {
                    try {
                        insertTransactionRecord(row, playerId, type, amount);
                    } catch (SQLException e) {
                        logger.warning(e,
                                "Failed to insert transaction record.");
                    }
                }
            });
        } catch (Exception e) {
            logger.warning(e, "Failed to delete shop record.");
        }
    }

    void insertTransactionRecord(Shop row, int playerId,
            TransactionType type, int amount) throws SQLException {
        Connection connection = database.getConnection();
        try (Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);

            shopTable.put(row);

            int shopId = shopTable.getId(row.world, row.x, row.y, row.z);
            insertTransaction.setInt(1, shopId);
            insertTransaction.setInt(2, playerId);
            insertTransaction.setString(3, type.toString().toLowerCase());
            insertTransaction.setInt(4, amount);
            insertTransaction.executeUpdate();

            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    static final BlockFace chestFaces[] = { BlockFace.SOUTH, BlockFace.WEST,
            BlockFace.NORTH, BlockFace.EAST, BlockFace.UP, BlockFace.DOWN };

    @EventHandler
    public void onInventoryClose(InventoryCloseEvent event) {
        InventoryHolder holder = event.getInventory().getHolder();

        Set<Sign> signs = new HashSet<Sign>();

        if (holder instanceof Chest) {
            addAttachingChestShopSigns((Chest) holder, signs);
        } else if (holder instanceof DoubleChest) {
            DoubleChest doubleChest = (DoubleChest) holder;
            addAttachingChestShopSigns(
                    (Chest)(doubleChest.getLeftSide()), signs);
            addAttachingChestShopSigns(
                    (Chest)(doubleChest.getRightSide()), signs);
        }

        if (signs.isEmpty()) {
            return;
        }

        final Connection connection = database.getConnection();

        try {
            final ArrayList<Shop> rows = new ArrayList<Shop>();
            for (Sign sign : signs) {
                rows.add(newShop(sign, sign.getLines()));
            }

            database.runAsynchronously(new Runnable() {
                public void run() {
                    try {
                        try {
                            connection.setAutoCommit(false);

                            shopTable.put(rows);

                            connection.commit();
                        } finally {
                            connection.setAutoCommit(true);
                        }

                    } catch (SQLException e) {
                        logger.warning(e, "Failed to enter a shop record.");
                    }
                }
            });
        } catch (Exception e) {
            logger.warning(e, "Failed to enter a shop record.");
        }
    }

    void addAttachingChestShopSigns(Chest chest, Set<Sign> target) {
        Block chestBlock = chest.getBlock();
        for (BlockFace face : chestFaces) {
            Block signBlock = chestBlock.getRelative(face);
            BlockState state = signBlock.getState();
            if (!(state instanceof Sign)) {
                continue;
            }

            Sign sign = (Sign) state;
            if (!ChestShopSign.isValid(sign)) {
                continue;
            }

            if (!chest.equals(uBlock.findConnectedChest(sign))) {
                continue;
            }

            target.add(sign);
        }
    }

    @Override
    public Shop validate(BlockState state) {
        if (! (state instanceof Sign)) {
            return null;
        }
        
        Sign sign = (Sign)state;
        if (! ChestShopSign.isValid(sign)) {
            return null;
        }
        
        try {
            return newShop(sign, sign.getLines());
        } catch (UnknownPlayerException | SQLException e) {
        }

        return null;
    }

}
