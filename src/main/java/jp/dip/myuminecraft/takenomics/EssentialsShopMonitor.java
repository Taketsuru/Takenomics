package jp.dip.myuminecraft.takenomics;

import java.sql.Connection;

import jp.dip.myuminecraft.takecore.DatabaseTask;
import jp.dip.myuminecraft.takecore.Logger;
import jp.dip.myuminecraft.takenomics.models.ShopTable;
import jp.dip.myuminecraft.takenomics.models.ShopTable.Shop;
import jp.dip.myuminecraft.takenomics.models.TransactionTable;
import net.ess3.api.IEssentials;
import net.ess3.api.events.SignBreakEvent;
import net.ess3.api.events.SignCreateEvent;

import org.bukkit.Bukkit;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.block.BlockState;
import org.bukkit.block.Sign;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.inventory.ItemStack;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.java.JavaPlugin;

import com.Acrobot.Breeze.Utils.MaterialUtil;
import com.earth2me.essentials.signs.EssentialsSign;

public class EssentialsShopMonitor implements Listener, ShopValidator {

    JavaPlugin        plugin;
    Logger            logger;
    CommandDispatcher commandDispatcher;
    Database          database;
    ShopTable         shopTable;
    TransactionTable  transactionTable;
    IEssentials       essentials;

    public EssentialsShopMonitor(JavaPlugin plugin, final Logger logger,
            Database database, ShopTable shopTable,
            TransactionTable transactionTable) {
        this.plugin = plugin;
        this.logger = logger;
        this.database = database;
        this.shopTable = shopTable;
        this.transactionTable = transactionTable;
    }

    public boolean enable() {
        if (database == null || shopTable == null
                || transactionTable == null) {
            logger.warning(
                    "No database connection.  Disable Essentials shop monitor.");
            return true;
        }

        Plugin ess = Bukkit.getPluginManager().getPlugin("Essentials");
        if (ess == null || !(ess instanceof IEssentials)) {
            logger.warning("Essentials is not found.");
            return true;
        }
        essentials = (IEssentials) ess;

        shopTable.addValidator(this);

        plugin.getServer().getPluginManager().registerEvents(this, plugin);

        return false;
    }

    public void disable() {
        if (shopTable != null) {
            shopTable.removeValidator(this);
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    void onSignCreateEvent(SignCreateEvent event) {
        Block block = event.getSign().getBlock();

        String lines[] = new String[4];
        for (int i = 0; i < lines.length; ++i) {
            lines[i] = event.getSign().getLine(i);
        }

        Shop shop = newShop(block.getWorld(), block.getX(), block.getY(),
                block.getZ(), lines);
        if (shop == null) {
            return;
        }

        database.submitAsync(new DatabaseTask() {
            public void run(Connection connection) throws Throwable {
                shopTable.put(connection, shop);
            }
        });
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    void onSignBreakEvent(SignBreakEvent event) {
        Block block = event.getSign().getBlock();
        final String world = block.getWorld().getName();
        final int x = block.getX();
        final int y = block.getY();
        final int z = block.getZ();

        database.submitAsync(new DatabaseTask() {
            public void run(Connection connection) throws Throwable {
                shopTable.remove(connection, world, x, y, z);
            }
        });
    }

    @Override
    public Shop validate(BlockState state) {
        if (!(state instanceof Sign)) {
            return null;
        }

        try {
            return newShop(state.getWorld(), state.getX(), state.getY(),
                    state.getZ(), ((Sign) state).getLines());
        } catch (Exception e) {
            logger.warning(e, "Failed to validate an essentials shop.");
        }

        return null;
    }

    Shop newShop(World world, int x, int y, int z, String[] lines) {

        String title = lines[0].trim();
        String signName = null;
        for (EssentialsSign signType : essentials.getSettings()
                .enabledSigns()) {
            if (!title.equalsIgnoreCase(signType.getSuccessName())) {
                continue;
            }
            if (signType.getName().equals("Buy")
                    || signType.getName().equals("Sell")) {
                signName = signType.getName();
                break;
            }
        }

        if (signName == null) {
            return null;
        }

        int quantity;
        try {
            quantity = Integer.parseInt(lines[1].trim());
        } catch (NumberFormatException e) {
            return null;
        }

        ItemStack item;
        try {
            item = essentials.getItemDb().get(lines[2]);
        } catch (Exception e) {
            return null;
        }
        if (item == null) {
            return null;
        }
        String material = MaterialUtil.getName(item);

        String priceLine = lines[3].trim();
        if (!priceLine.matches("^[^0-9-\\.][\\.0-9]+$")) {
            return null;
        }
        double price;
        try {
            price = Double.parseDouble(priceLine.substring(1));
        } catch (NumberFormatException e) {
            return null;
        }

        double buyPrice = -1;
        double sellPrice = -1;
        if (signName.equals("Buy")) {
            buyPrice = price;
        } else {
            sellPrice = price;
        }

        return new Shop(x, z, y, world.getName(), null, quantity, material,
                buyPrice, sellPrice, -1, -1);
    }

}
