package jp.dip.myuminecraft.takenomics;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.Location;
import org.bukkit.OfflinePlayer;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.block.BlockState;
import org.bukkit.block.Sign;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerInteractEvent;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;

import com.sk89q.worldedit.EditSession;
import com.sk89q.worldedit.LocalConfiguration;
import com.sk89q.worldedit.Vector;
import com.sk89q.worldedit.WorldEdit;
import com.sk89q.worldedit.bukkit.BukkitWorld;
import com.sk89q.worldedit.bukkit.selections.CuboidSelection;
import com.sk89q.worldedit.bukkit.selections.Polygonal2DSelection;
import com.sk89q.worldedit.regions.Region;
import com.sk89q.worldedit.world.snapshot.Snapshot;
import com.sk89q.worldedit.world.snapshot.SnapshotRepository;
import com.sk89q.worldedit.world.snapshot.SnapshotRestore;
import com.sk89q.worldedit.world.storage.ChunkStore;
import com.sk89q.worldguard.bukkit.BukkitPlayer;
import com.sk89q.worldguard.bukkit.WorldGuardPlugin;
import com.sk89q.worldguard.domains.DefaultDomain;
import com.sk89q.worldguard.protection.managers.RegionManager;
import com.sk89q.worldguard.protection.regions.ProtectedCuboidRegion;
import com.sk89q.worldguard.protection.regions.ProtectedPolygonalRegion;
import com.sk89q.worldguard.protection.regions.ProtectedRegion;

import jp.dip.myuminecraft.takecore.DatabaseTask;
import jp.dip.myuminecraft.takecore.Logger;
import jp.dip.myuminecraft.takecore.ManagedSign;
import jp.dip.myuminecraft.takecore.Messages;
import jp.dip.myuminecraft.takecore.MisconfigurationException;
import jp.dip.myuminecraft.takecore.SignTable;
import jp.dip.myuminecraft.takecore.SignTableListener;
import jp.dip.myuminecraft.takecore.TakeCore;
import jp.dip.myuminecraft.takenomics.models.PlayerTable;
import jp.dip.myuminecraft.takenomics.models.WorldTable;
import net.milkbowl.vault.economy.Economy;
import net.milkbowl.vault.economy.EconomyResponse;
import net.milkbowl.vault.economy.EconomyResponse.ResponseType;
import net.milkbowl.vault.permission.Permission;

public class LandRentalManager implements Listener, SignTableListener {

    private enum RentalState {
        PENDING, READY, SUSPENDED
    }

    private final class Rental {
        String      worldName;
        String      regionName;
        int         signX;
        int         signY;
        int         signZ;
        RentalState state;
        int         fee;
        int         maxPrepayment;
        Contract    currentContract;
        RentalSign  sign;

        Rental(String worldName, String regionName, RentalState state, int x,
                int y, int z, int fee, int maxPrepayment) {
            this.worldName = worldName;
            this.regionName = regionName;
            this.signX = x;
            this.signY = y;
            this.signZ = z;
            this.state = state;
            this.fee = fee;
            this.maxPrepayment = maxPrepayment;
            this.currentContract = null;
            this.sign = null;
        }
    }

    private final class Contract {
        UUID   tenant;
        String expiration;
        int    prepayment;

        Contract(UUID tenant, String expiration, int prepayment) {
            this.tenant = tenant;
            this.expiration = expiration;
            this.prepayment = prepayment;
        }
    }

    private final class RentalSign extends ManagedSign {
        Rental rental;

        public RentalSign(SignTableListener owner, Location location,
                Location attachedLocation, Rental rental) {
            super(owner, location, attachedLocation);
            this.rental = rental;
        }
    }

    private final class TerminatedContract {
        String  worldName;
        String  regionName;
        int     fee;
        String  creationDate;
        boolean terminated;

        TerminatedContract(String worldName, String regionName, int fee,
                String creationDate, boolean terminated) {
            this.worldName = worldName;
            this.regionName = regionName;
            this.fee = fee;
            this.creationDate = creationDate;
            this.terminated = terminated;
        }
    }

    private final class SignLocation {
        String worldName;
        int    x;
        int    y;
        int    z;

        SignLocation(String worldName, int x, int y, int z) {
            this.worldName = worldName;
            this.x = x;
            this.y = y;
            this.z = z;
        }

        @Override
        public boolean equals(Object arg) {
            if (!(arg instanceof SignLocation)) {
                return false;
            }
            SignLocation loc = (SignLocation) arg;
            return worldName.equals(loc.worldName) && x == loc.x && y == loc.y
                    && z == loc.z;
        }

        @Override
        public int hashCode() {
            int result = 0;
            int accx = x;
            int accy = y;
            int accz = z;
            for (int i = 0; i < 10; ++i) {
                result = (result << 3) | ((accx & 1) << 2) | ((accy & 1) << 1)
                        | (accz & 1);
                accx >>= 1;
                accy >>= 1;
                accz >>= 1;
            }
            result = ((result >> 16) ^ result) * 0x45d9f3b;
            result = ((result >> 16) ^ result) * 0x45d9f3b;
            result = ((result >> 16) ^ result);
            result ^= worldName.hashCode();

            return result;
        }
    }

    static final int              currentRentalSchemaVersion   = 1;
    static final int              currentContractSchemaVersion = 1;
    static final long             maxTaskExecutionTime         = 20;
    static final String           header                       = "[re.rental]";
    static final String           permNodeToCreateSign         = "takenomics.land_rental.create";
    static final String           permNodeToCreateSignOwn      = "takenomics.land_rental.create.own";
    static final int              maxFee                       = 9999999;
    static final int              maxMaxPrepayment             = 9;
    static final Pattern          feePattern                   = Pattern
            .compile("^\\$?[ ]*([1-9][0-9]*)([ ]*/[ ]*[^0-9]*)?$");
    static final Pattern          maxPrepaymentPattern         = Pattern
            .compile("^[ ]*\\+?[ ]*([1-9][0-9]*)[ ]*$");
    static final int              dailyMaintenanceTime         = 8;
    static final SimpleDateFormat dateFormat                   = new SimpleDateFormat(
            "yyyy-MM-dd");

    JavaPlugin                    plugin;
    Logger                        logger;
    Messages                      messages;
    Economy                       economy;
    Permission                    permission;
    SignTable                     signTable;
    Database                      database;
    String                        rentalTableName;
    String                        contractTableName;
    PlayerTable                   playerTable;
    WorldTable                    worldTable;
    SnapshotRepository            snapshotRepository;
    Map<SignLocation, Rental>     signLocationToRental         = new HashMap<>();
    Map<String, Rental>           rentals                      = new HashMap<>();
    Map<Location, RentalSign>     loadedSigns                  = new HashMap<>();

    public LandRentalManager(JavaPlugin plugin, Logger logger,
            Messages messages, Economy economy, Permission permission,
            Database database, PlayerTable playerTable,
            WorldTable worldTable) {
        this.plugin = plugin;
        this.logger = logger;
        this.messages = messages;
        this.economy = economy;
        this.permission = permission;
        this.database = database;
        this.playerTable = playerTable;
        this.worldTable = worldTable;
    }

    public void enable() throws Throwable {
        TakeCore takeCore = (TakeCore) plugin.getServer().getPluginManager()
                .getPlugin("TakeCore");
        signTable = takeCore.getSignTable();

        LocalConfiguration config = WorldEdit.getInstance().getConfiguration();
        if (config.snapshotRepo == null) {
            throw new Exception(
                    "WorldEdit snapshot repository is not configured.");
        }

        database.submitSync(new DatabaseTask() {
            @Override
            public void run(Connection connection)
                    throws SQLException, MisconfigurationException {
                rentalTableName = database.getTablePrefix() + "land_rentals";
                contractTableName = database.getTablePrefix()
                        + "land_rental_contracts";

                initializeRentalTable(connection);
                initializeContractTable(connection);
                processExpiration(connection);
                loadRentals(connection);
            }
        });

        signTable.addListener(this);
        Bukkit.getPluginManager().registerEvents(this, plugin);
    }

    public void disable() {
        signTable.removeListener(this);
        signLocationToRental.clear();
        rentals.clear();
        loadedSigns.clear();
    }

    public UUID findTaxPayer(String worldName, ProtectedRegion region) {
        Rental rental = findRental(worldName, region.getId());
        if (rental != null && rental.currentContract != null) {
            return rental.currentContract.tenant;
        }

        DefaultDomain domain = region.getOwners();
        if (domain == null || domain.size() == 0) {
            return null;
        }

        double maxBalance = Double.MIN_VALUE;
        UUID payer = null;
        for (UUID uuid : domain.getUniqueIds()) {
            double balance = economy.getBalance(Bukkit.getOfflinePlayer(uuid));
            if (maxBalance < balance) {
                maxBalance = balance;
                payer = uuid;
            }
        }

        return payer;
    }

    @Override
    public boolean mayCreate(Player player, Location location,
            Location attachedLocation, String[] lines) {
        if (!isLandRentalSign(lines)) {
            return true;
        }

        boolean perm = player.hasPermission(permNodeToCreateSign);
        boolean permOwn = player.hasPermission(permNodeToCreateSignOwn);
        if (!perm && !permOwn) {
            messages.send(player, "landRentalCreationPermDenied");
            return false;
        }

        ProtectedRegion foundRegion = findRegion(player, attachedLocation);
        if (foundRegion == null) {
            messages.send(player, "landRentalNoRegion");
            return false;
        }

        World world = attachedLocation.getWorld();
        String worldName = world.getName();
        String regionName = foundRegion.getId();
        Rental rental = findRental(worldName, regionName);
        if (rental != null && hasSign(rental)) {
            messages.send(player, "landRentalAlreadyRented");
            return false;
        }

        return true;
    }

    @Override
    public ManagedSign create(Player player, Location location,
            Location attachedLocation, String[] lines) {
        if (!isLandRentalSign(lines)) {
            return null;
        }

        Rental rental = null;

        if (player == null) {
            rental = signLocationToRental.get(new SignLocation(
                    location.getWorld().getName(), location.getBlockX(),
                    location.getBlockY(), location.getBlockZ()));
            if (rental == null) {
                clearSign(location.getBlock());
                return null;
            }

            String[] newLines = new String[lines.length];
            newLines[0] = lines[0];
            for (int i = 1; i < newLines.length; ++i) {
                newLines[i] = "";
            }
            updateSign(rental, newLines);

            boolean changed = false;
            for (int i = 1; i < newLines.length; ++i) {
                if (!newLines[i].equals(lines[i])) {
                    lines[i] = newLines[i];
                    changed = true;
                }
            }

            if (changed) {
                location.getBlock().getState().update();
            }

        } else {
            ProtectedRegion region = findRegion(player, attachedLocation);
            if (region == null) {
                clearSign(lines);
                return null;
            }

            String worldName = attachedLocation.getWorld().getName();
            String regionName = region.getId();
            rental = findRental(worldName, regionName);

            int intValue = 0;
            if (lines[1].length() != 0) {
                try {
                    Matcher match = feePattern.matcher(lines[1]);
                    if (!match.matches()) {
                        throw new NumberFormatException();
                    }

                    intValue = Integer.parseInt(match.group(1));
                    if (intValue <= 0 || maxFee < intValue) {
                        throw new NumberFormatException();
                    }

                } catch (NumberFormatException e) {
                    messages.send(player, "landRentalInvalidFee", maxFee);
                    intValue = 0;
                }

            } else if (rental != null) {
                intValue = rental.fee;

            } else {
                messages.send(player, "landRentalInvalidFee", maxFee);
            }

            int fee = intValue;

            if (lines[2].length() != 0) {
                try {
                    Matcher match = maxPrepaymentPattern.matcher(lines[2]);
                    if (!match.matches()) {
                        throw new NumberFormatException();
                    }

                    intValue = Integer.parseInt(match.group(1));
                    if (intValue < 0 || maxMaxPrepayment < intValue) {
                        throw new NumberFormatException();
                    }

                } catch (NumberFormatException e) {
                    messages.send(player, "landRentalInvalidMaxPrepayment",
                            maxMaxPrepayment);
                    intValue = 0;
                }

            } else if (rental != null) {
                intValue = rental.maxPrepayment;

            } else {
                messages.send(player, "landRentalInvalidMaxPrepayment",
                        maxMaxPrepayment);
            }

            int maxPrepayment = intValue;

            if (fee == 0 || maxPrepayment == 0) {
                for (int i = 1; i < 4; ++i) {
                    lines[i] = "";
                }
                return null;
            }

            if (rental == null) {
                rental = createRental(player, worldName, regionName, location,
                        fee, maxPrepayment);

            } else if (rental.currentContract == null) {
                if (rental.fee != fee
                        || rental.maxPrepayment != maxPrepayment) {
                    rental.fee = fee;
                    rental.maxPrepayment = maxPrepayment;

                    database.submitAsync(new DatabaseTask() {
                        @Override
                        public void run(Connection connection)
                                throws SQLException {
                            try (PreparedStatement stmt = connection
                                    .prepareStatement(String.format(
                                            "UPDATE %s AS rental, %s AS world "
                                                    + "SET rental.fee = ?,"
                                                    + " rental.max_prepayment = ? "
                                                    + "WHERE rental.world = world.id"
                                                    + " AND rental.region = ?"
                                                    + " AND world.name = ?",
                                            rentalTableName,
                                            worldTable.getTableName()))) {
                                stmt.setInt(1, fee);
                                stmt.setInt(2, maxPrepayment);
                                stmt.setString(3, regionName);
                                stmt.setString(4, worldName);
                                stmt.execute();
                            }
                        }
                    });
                }

            } else {
                if (rental.fee != fee) {
                    messages.send(player, "landRentalSpecifiedFeeIgnored");
                }

                if (rental.maxPrepayment != maxPrepayment) {
                    messages.send(player,
                            "landRentalSpecifiedMaxPrepaymentIgnored");
                }
            }

            updateSign(rental, lines);
        }

        RentalSign sign = new RentalSign(this, location, attachedLocation,
                rental);
        loadedSigns.put(location, sign);
        rental.sign = sign;

        return sign;
    }

    @Override
    public void destroy(Player player, ManagedSign sign) {
        RentalSign rentalSign = (RentalSign) sign;
        Rental rental = rentalSign.rental;

        loadedSigns.remove(sign.getLocation());
        rental.sign = null;

        if (player == null) {
            return;
        }

        Contract contract = rental.currentContract;
        if (contract != null) {
            messages.send(player, "landRentalEffectiveSignDestroyed");
            cancelPrepayment(rental);
        }
    }

    @EventHandler(priority = EventPriority.LOW, ignoreCancelled = true)
    public void onPlayerInteract(PlayerInteractEvent event) {
        Block signBlock = event.getClickedBlock();
        if (!(signBlock.getState() instanceof Sign)) {
            return;
        }

        RentalSign sign = loadedSigns.get(signBlock.getLocation());
        if (sign == null) {
            return;
        }

        Rental rental = sign.rental;
        Contract contract = rental.currentContract;

        WorldGuardPlugin worldGuard = WorldGuardPlugin.inst();
        Player player = event.getPlayer();
        BukkitPlayer wgPlayer = new BukkitPlayer(worldGuard, player);
        RegionManager wgRegionManager = worldGuard
                .getRegionManager(signBlock.getWorld());
        ProtectedRegion region = wgRegionManager.getRegion(rental.regionName);

        if (region == null) {
            destroyRental(rental);

        } else if (region.getOwners().contains(wgPlayer)) {
            switch (event.getAction()) {
            case RIGHT_CLICK_BLOCK:
                toggleRentalState(rental);
                break;

            default:
                return;
            }

        } else {
            switch (event.getAction()) {
            case RIGHT_CLICK_BLOCK:
                pay(player, rental);
                break;

            case LEFT_CLICK_BLOCK:
                if (contract != null
                        && contract.tenant.equals(player.getUniqueId())) {
                    cancelPrepayment(rental);
                }
                break;

            default:
                return;
            }
        }
    }

    boolean isLandRentalSign(String[] lines) {
        return ChatColor.stripColor(lines[0]).equalsIgnoreCase(header);
    }

    Rental findRental(String worldName, String regionName) {
        return rentals.get(worldName + " " + regionName);
    }

    ProtectedRegion findRegion(Player player, Location attachedLocation) {
        boolean perm = player.hasPermission(permNodeToCreateSign);
        WorldGuardPlugin worldGuard = WorldGuardPlugin.inst();
        RegionManager wgRegionManager = worldGuard
                .getRegionManager(attachedLocation.getWorld());
        BukkitPlayer wgPlayer = new BukkitPlayer(worldGuard, player);

        ProtectedRegion foundRegion = null;
        int currentPriority = Integer.MIN_VALUE;
        Location rgLocation = attachedLocation.clone();
        for (int mody = -1; mody <= 1; ++mody) {
            for (int modz = -1; modz <= 1; ++modz) {
                for (int modx = -1; modx <= 1; ++modx) {
                    rgLocation.setX(attachedLocation.getBlockX() + modx);
                    rgLocation.setY(attachedLocation.getBlockY() + mody);
                    rgLocation.setZ(attachedLocation.getBlockZ() + modz);

                    for (ProtectedRegion region : wgRegionManager
                            .getApplicableRegions(rgLocation)) {
                        if (region.getId().equals("__global__")) {
                            continue;
                        }

                        int regionPriority = region.getPriority();
                        if (regionPriority <= currentPriority) {
                            continue;
                        }

                        DefaultDomain owners = region.getOwners();
                        if (!perm && !owners.contains(wgPlayer)) {
                            continue;
                        }

                        foundRegion = region;
                        currentPriority = regionPriority;
                    }
                }
            }
        }

        return foundRegion;
    }

    Rental createRental(Player player, String worldName, String regionName,
            Location location, int fee, int maxPrepayment) {
        Rental rental = new Rental(worldName, regionName, RentalState.PENDING,
                location.getBlockX(), location.getBlockY(),
                location.getBlockZ(), fee, maxPrepayment);
        signLocationToRental
                .put(new SignLocation(worldName, location.getBlockX(),
                        location.getBlockY(), location.getBlockZ()), rental);
        rentals.put(worldName + " " + regionName, rental);

        database.submitAsync(new DatabaseTask() {
            @Override
            public void run(Connection connection) throws SQLException {
                try (PreparedStatement stmt = connection
                        .prepareStatement(String.format(
                                "INSERT INTO %s "
                                        + "(world,region,x,y,z,state,fee,max_prepayment) "
                                        + "SELECT worlds.id,?,?,?,?,'pending',?,? "
                                        + "FROM %s AS worlds WHERE worlds.name = ?",
                                rentalTableName, worldTable.getTableName()))) {
                    stmt.setString(1, regionName);
                    stmt.setInt(2, location.getBlockX());
                    stmt.setInt(3, location.getBlockY());
                    stmt.setInt(4, location.getBlockZ());
                    stmt.setInt(5, fee);
                    stmt.setInt(6, maxPrepayment);
                    stmt.setString(7, worldName);
                    stmt.execute();
                }
            }
        });

        return rental;
    }

    void destroyRental(Rental rental) {
        cancelPrepayment(rental);

        rentals.remove(rental.worldName + " " + rental.regionName);
        signLocationToRental.remove(new SignLocation(rental.worldName,
                rental.signX, rental.signY, rental.signZ));
        RentalSign sign = loadedSigns
                .get(new Location(Bukkit.getWorld(rental.worldName),
                        rental.signX, rental.signY, rental.signZ));
        if (sign != null) {
            clearSign(sign.getLocation().getBlock());
        }

        database.submitAsync(new DatabaseTask() {
            @Override
            public void run(Connection connection) throws SQLException {
                if (rental.currentContract != null) {
                    try (PreparedStatement stmt = connection
                            .prepareStatement(String.format(
                                    "DELETE rental, contract "
                                            + "FROM %s AS rental LEFT JOIN %s AS contract"
                                            + " ON contract.id = rental.current_contract"
                                            + " INNER JOIN %s AS world ON rental.world = world.id"
                                            + "WHERE world.name = ? AND rental.region = ?",
                                    rentalTableName, contractTableName,
                                    worldTable.getTableName()))) {
                        stmt.setString(1, rental.worldName);
                        stmt.setString(2, rental.regionName);
                    }
                } else {
                    try (PreparedStatement stmt = connection
                            .prepareStatement(String.format(
                                    "DELETE rental "
                                            + "FROM %s AS contract INNER JOIN %s AS world"
                                            + " ON world.id = rental.world "
                                            + "WHERE world.name = ? AND rental.region = ?",
                                    rentalTableName,
                                    worldTable.getTableName()))) {
                        stmt.setString(1, rental.worldName);
                        stmt.setString(2, rental.regionName);
                    }
                }
            }
        });
    }

    boolean hasSign(Rental rental) {
        BlockState state = Bukkit.getWorld(rental.worldName)
                .getBlockAt(rental.signX, rental.signY, rental.signZ)
                .getState();

        return state instanceof Sign
                && isLandRentalSign(((Sign) state).getLines());
    }

    void toggleRentalState(Rental rental) {
        RentalState newState;
        switch (rental.state) {
        case PENDING:
            return;
        case READY:
            newState = RentalState.SUSPENDED;
            if (rental.currentContract != null) {
                cancelPrepayment(rental);
            }
            break;
        case SUSPENDED:
            newState = RentalState.READY;
            break;
        default:
            throw new AssertionError();
        }

        rental.state = newState;
        if (rental.sign != null) {
            updateSign(rental.sign);
        }

        database.submitAsync(new DatabaseTask() {
            @Override
            public void run(Connection connection) throws SQLException {
                try (PreparedStatement stmt = connection
                        .prepareStatement(String.format(
                                "UPDATE %s AS rental, %s AS world"
                                        + "SET rental.state = ?"
                                        + "WHERE rental.world = world.id"
                                        + " AND rental.region = ? AND world.name = ?",
                                rentalTableName, worldTable.getTableName()))) {
                    stmt.setString(1, newState.toString().toLowerCase());
                    stmt.setString(2, rental.regionName);
                    stmt.setString(3, rental.worldName);
                    stmt.execute();
                }
            }
        });
    }

    void pay(Player player, Rental rental) {
        Contract contract = rental.currentContract;
        if (rental.state != RentalState.READY) {
            return;
        }

        if (contract == null) {
            makeContract(player, rental);

        } else if (contract.tenant.equals(player.getUniqueId())) {
            prepay(rental);
        }
    }

    void makeContract(Player player, Rental rental) {
        EconomyResponse response = economy.withdrawPlayer(player, rental.fee);
        if (response.type != ResponseType.SUCCESS) {
            messages.send(player, "landRentalFailedToPay");
            return;
        }

        pay(rental.worldName, rental.regionName, rental.fee);

        World world = Bukkit.getWorld(rental.worldName);
        WorldGuardPlugin worldGuard = WorldGuardPlugin.inst();
        RegionManager wgRegionManager = worldGuard.getRegionManager(world);
        ProtectedRegion region = wgRegionManager.getRegion(rental.regionName);
        DefaultDomain members = region.getMembers();
        members.clear();
        members.addPlayer(player.getUniqueId());

        Calendar expiration = Calendar.getInstance();
        expiration.add(Calendar.HOUR_OF_DAY, -8);
        expiration.set(Calendar.HOUR_OF_DAY, 8);
        expiration.set(Calendar.MINUTE, 0);
        expiration.set(Calendar.SECOND, 0);
        expiration.add(Calendar.DAY_OF_YEAR, 7);
        Contract contract = new Contract(player.getUniqueId(),
                dateFormat.format(new Date(expiration.getTimeInMillis())), 0);
        rental.currentContract = contract;
        if (rental.sign != null) {
            updateSign(rental.sign);
        }

        byte[] uuid = new byte[Constants.sizeOfUUID];
        Database.toBytes(player.getUniqueId(), uuid);

        database.submitAsync(new DatabaseTask() {
            @Override
            public void run(Connection connection) throws Throwable {
                try (PreparedStatement stmt = connection
                        .prepareStatement(String.format(
                                "SELECT contract.expiration "
                                        + "FROM %s AS contract"
                                        + " JOIN %s AS world ON contract.world = world.id"
                                        + " JOIN %s AS player ON contract.tenant = player.id "
                                        + "WHERE contract.state = 'end' AND world.name = ?"
                                        + " AND contract.region = ? AND player.uuid = ? "
                                        + "ORDER BY contract.expiration DESC LIMIT 1",
                                contractTableName, worldTable.getTableName(),
                                playerTable.getTableName()))) {
                    stmt.setString(1, rental.worldName);
                    stmt.setString(2, rental.regionName);
                    stmt.setBytes(3, uuid);
                    ResultSet rs = stmt.executeQuery();
                    if (rs.next()) {
                        String expiration = rs.getString(1);
                        new BukkitRunnable() {
                            @Override
                            public void run() {
                                try {
                                    restoreBlocks(rental.worldName,
                                            rental.regionName, expiration);
                                } catch (Throwable e) {
                                    logger.warning(e,
                                            "Failed to restore the last block state: "
                                                    + "world %s region %s",
                                            rental.worldName,
                                            rental.regionName);
                                }
                            }
                        }.runTask(plugin);
                    }
                }
            }
        });

        database.submitAsync(new DatabaseTask() {
            @Override
            public void run(Connection connection) throws SQLException {
                connection.setAutoCommit(false);

                try {

                    int contractId = 0;
                    try (PreparedStatement stmt = connection.prepareStatement(
                            String.format(
                                    "INSERT INTO %s "
                                            + "(region, world, state, tenant, expiration, prepayment) "
                                            + "SELECT ?, world.id, 'effective', player.id, ?, 0 "
                                            + "FROM %s AS world JOIN %s AS player "
                                            + "WHERE world.name = ? AND player.uuid = ?",
                                    contractTableName,
                                    worldTable.getTableName(),
                                    playerTable.getTableName()),
                            Statement.RETURN_GENERATED_KEYS)) {
                        stmt.setString(1, rental.regionName);
                        stmt.setString(2, contract.expiration);
                        stmt.setString(3, rental.worldName);
                        stmt.setBytes(4, uuid);
                        stmt.executeUpdate();
                        ResultSet rs = stmt.getGeneratedKeys();
                        rs.next();
                        contractId = rs.getInt(1);
                    }

                    try (PreparedStatement stmt = connection
                            .prepareStatement(String.format(
                                    "UPDATE %s AS rental, %s AS world "
                                            + "SET rental.current_contract = ? "
                                            + "WHERE world.name = ?"
                                            + " AND rental.world = world.id"
                                            + " AND rental.region = ?",
                                    rentalTableName,
                                    worldTable.getTableName()))) {
                        stmt.setInt(1, contractId);
                        stmt.setString(2, rental.worldName);
                        stmt.setString(3, rental.regionName);
                        stmt.execute();
                    }
                } finally {
                    connection.setAutoCommit(true);
                }
            }
        });
    }

    void terminateContract(TerminatedContract contract) throws Exception {
        WorldGuardPlugin.inst()
                .getRegionManager(Bukkit.getWorld(contract.worldName))
                .getRegion(contract.regionName).getMembers().clear();

        restoreBlocks(contract.worldName, contract.regionName,
                contract.creationDate);
    }

    void restoreBlocks(String worldName, String regionName, String date)
            throws Exception {
        World world = Bukkit.getServer().getWorld(worldName);

        WorldGuardPlugin worldGuard = WorldGuardPlugin.inst();
        RegionManager wgRegionManager = worldGuard.getRegionManager(world);
        ProtectedRegion protectedRegion = wgRegionManager
                .getRegion(regionName);

        BukkitWorld weWorld = new BukkitWorld(world);
        EditSession editSession = WorldEdit.getInstance()
                .getEditSessionFactory()
                .getEditSession((com.sk89q.worldedit.world.World) weWorld, -1);

        Region region;
        if (protectedRegion instanceof ProtectedCuboidRegion) {
            ProtectedCuboidRegion cuboid = (ProtectedCuboidRegion) protectedRegion;
            Vector pt1 = cuboid.getMinimumPoint();
            Vector pt2 = cuboid.getMaximumPoint();
            CuboidSelection selection = new CuboidSelection(world, pt1, pt2);
            region = selection.getRegionSelector().getRegion();

        } else if (protectedRegion instanceof ProtectedPolygonalRegion) {
            ProtectedPolygonalRegion poly2d = (ProtectedPolygonalRegion) protectedRegion;
            Polygonal2DSelection selection = new Polygonal2DSelection(world,
                    poly2d.getPoints(), poly2d.getMinimumPoint().getBlockY(),
                    poly2d.getMaximumPoint().getBlockY());
            region = selection.getRegionSelector().getRegion();

        } else {
            throw new AssertionError("Unknown region type: "
                    + protectedRegion.getClass().getCanonicalName());
        }

        LocalConfiguration config = WorldEdit.getInstance().getConfiguration();
        Snapshot snapshot = config.snapshotRepo
                .getSnapshot(String.format("%s/%s", worldName, date));
        ChunkStore chunkStore = snapshot.getChunkStore();

        try {
            SnapshotRestore restore = new SnapshotRestore(chunkStore,
                    editSession, region);
            restore.restore();

            if (restore.hadTotalFailure()) {
                String error = restore.getLastErrorMessage();
                throw new Exception(
                        error != null ? error : "WorldEdit restore failed");
            }
        } finally {
            chunkStore.close();
        }
    }

    void prepay(Rental rental) {
        Contract contract = rental.currentContract;

        if (rental.maxPrepayment <= contract.prepayment) {
            return;
        }

        OfflinePlayer tenant = Bukkit.getOfflinePlayer(contract.tenant);

        EconomyResponse response = economy.withdrawPlayer(tenant, rental.fee);
        if (response.type != ResponseType.SUCCESS) {
            Player onlineTenant = tenant.getPlayer();
            if (onlineTenant != null) {
                messages.send(onlineTenant, "landRentalFailedToPay");
            }
            return;
        }

        int newPrepayment = ++contract.prepayment;
        if (rental.sign != null) {
            updateSign(rental.sign);
        }

        database.submitAsync(new DatabaseTask() {
            @Override
            public void run(Connection connection) throws SQLException {
                try (PreparedStatement stmt = connection
                        .prepareStatement(String.format(
                                "UPDATE %s AS contract, %s AS world "
                                        + "SET contract.prepayment = ? "
                                        + "WHERE contract.world = world.id"
                                        + " AND world.name = ? AND contract.region = ?",
                                contractTableName,
                                worldTable.getTableName()))) {
                    stmt.setInt(1, newPrepayment);
                    stmt.setString(2, rental.worldName);
                    stmt.setString(3, rental.regionName);
                    stmt.execute();
                }
            }
        });
    }

    void cancelPrepayment(Rental rental) {
        Contract contract = rental.currentContract;

        if (contract == null || contract.prepayment <= 0) {
            return;
        }

        OfflinePlayer tenant = Bukkit.getOfflinePlayer(contract.tenant);

        EconomyResponse response = economy.depositPlayer(tenant,
                rental.fee * contract.prepayment);
        if (response.type != ResponseType.SUCCESS) {
            Player onlineTenant = tenant.getPlayer();
            if (onlineTenant != null) {
                messages.send(onlineTenant,
                        "landRentalFailedToCancelPrepayment");
            }
            return;
        }

        contract.prepayment = 0;
        if (rental.sign != null) {
            updateSign(rental.sign);
        }

        database.submitAsync(new DatabaseTask() {
            @Override
            public void run(Connection connection) throws SQLException {
                try (PreparedStatement stmt = connection
                        .prepareStatement(String.format(
                                "UPDATE %s AS contract, %s AS rental, %s AS world "
                                        + "SET contract.prepayment = 0 "
                                        + "WHERE rental.current_contract = contract.id"
                                        + " AND rental.world = world.id"
                                        + " AND rental.region = ?"
                                        + " AND world.name = ?",
                                contractTableName, rentalTableName,
                                worldTable.getTableName()))) {
                    stmt.setString(1, rental.regionName);
                    stmt.setString(2, rental.worldName);
                    stmt.execute();
                }
            }
        });
    }

    void clearSign(Block signBlock) {
        BlockState state = signBlock.getState();
        if (state instanceof Sign && clearSign(((Sign) state).getLines())) {
            state.update();
        }
    }

    boolean clearSign(String[] lines) {
        boolean needUpdate = false;
        for (int i = 1; i < 4; ++i) {
            if (0 < lines.length) {
                lines[i] = "";
                needUpdate = true;
            }
        }
        return needUpdate;
    }

    void initializeRentalTable(Connection connection)
            throws SQLException, MisconfigurationException {
        if (!database.hasTable(connection, rentalTableName)) {
            createRentalTable(connection, rentalTableName);

        } else {
            int version = database.getVersion(connection, rentalTableName);
            switch (version) {
            case currentRentalSchemaVersion:
                break;

            default:
                throw new MisconfigurationException(String.format(
                        "Unknown table schema version: table %s, version %d",
                        rentalTableName, version));
            }
        }
    }

    void initializeContractTable(Connection connection)
            throws SQLException, MisconfigurationException {
        if (!database.hasTable(connection, contractTableName)) {
            createContractTable(connection, contractTableName);

        } else {
            int version = database.getVersion(connection, contractTableName);
            switch (version) {
            case currentContractSchemaVersion:
                break;

            default:
                throw new MisconfigurationException(String.format(
                        "Unknown table schema version: table %s, version %d",
                        contractTableName, version));
            }
        }
    }

    void createRentalTable(Connection connection, String newTableName)
            throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                    "CREATE TABLE %s (region VARCHAR(255) NOT NULL,"
                            + "world TINYINT UNSIGNED NOT NULL,"
                            + "x SMALLINT NOT NULL," + "z SMALLINT NOT NULL,"
                            + "y SMALLINT NOT NULL,"
                            + "state ENUM('pending', 'ready', 'suspended') NOT NULL,"
                            + "creation_date DATE,"
                            + "fee MEDIUMINT UNSIGNED NOT NULL,"
                            + "max_prepayment TINYINT UNSIGNED NOT NULL,"
                            + "current_contract MEDIUMINT UNSIGNED,"
                            + "PRIMARY KEY (region, world), INDEX (state))",
                    newTableName));
        }
        database.setVersion(connection, rentalTableName,
                currentRentalSchemaVersion);
    }

    void createContractTable(Connection connection, String newTableName)
            throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                    "CREATE TABLE %s ("
                            + "id MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                            + "region VARCHAR(255) NOT NULL,"
                            + "world TINYINT UNSIGNED NOT NULL,"
                            + "state ENUM('effective', 'terminating', 'end') NOT NULL,"
                            + "tenant MEDIUMINT UNSIGNED NOT NULL,"
                            + "expiration DATE NOT NULL,"
                            + "prepayment TINYINT UNSIGNED NOT NULL,"
                            + "INDEX (region, world, tenant, expiration), INDEX (state))",
                    newTableName));
        }
        database.setVersion(connection, contractTableName,
                currentContractSchemaVersion);
    }

    void loadRentals(Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery(String.format(
                    "SELECT world.name, rental.region, rental.x, rental.y, rental.z,"
                            + " rental.state, rental.fee, rental.max_prepayment,"
                            + " player.uuid, contract.expiration, contract.prepayment "
                            + "FROM %s AS rental JOIN %s AS world ON rental.world = world.id"
                            + " LEFT JOIN %s AS contract ON rental.current_contract = contract.id"
                            + " LEFT JOIN %s AS player ON contract.tenant = player.id",
                    rentalTableName, worldTable.getTableName(),
                    contractTableName, playerTable.getTableName()));
            while (rs.next()) {
                String worldName = rs.getString(1);
                String regionName = rs.getString(2);
                int x = rs.getInt(3);
                int y = rs.getInt(4);
                int z = rs.getInt(5);
                RentalState rentalState = RentalState
                        .valueOf(rs.getString(6).toUpperCase());
                int fee = rs.getInt(7);
                int maxPrepayment = rs.getInt(8);
                byte[] tenantId = rs.getBytes(9);
                String expiration = rs.getString(10);
                int prepayment = rs.getInt(11);

                Rental rental = new Rental(worldName, regionName, rentalState,
                        x, y, z, fee, maxPrepayment);
                Contract contract = tenantId == null ? null
                        : new Contract(Database.toUUID(tenantId), expiration,
                                prepayment);
                rental.currentContract = contract;
                rentals.put(worldName + " " + regionName, rental);
                signLocationToRental.put(new SignLocation(worldName, x, y, z),
                        rental);
            }
        }
    }

    void processExpiration(Connection connection) throws SQLException {
        Queue<TerminatedContract> worklist = new ArrayDeque<>();

        try (Statement stmt = connection.createStatement()) {
            ResultSet resultSet = stmt.executeQuery(String.format(
                    "SELECT world.name, rental.region, rental.fee,"
                            + " rental.creation_date, contract.prepayment "
                            + "FROM %s AS contract"
                            + " INNER JOIN %s AS rental"
                            + " ON contract.id = rental.current_contract"
                            + " INNER JOIN %s AS world"
                            + " ON rental.world = world.id "
                            + "WHERE contract.state = 'terminating'",
                    contractTableName, rentalTableName,
                    worldTable.getTableName()));

            while (resultSet.next()) {
                String worldName = resultSet.getString(1);
                String regionName = resultSet.getString(2);
                int fee = resultSet.getInt(3);
                String creationDate = resultSet.getString(4);
                boolean terminated = resultSet.getInt(5) == 0;
                worklist.add(new TerminatedContract(worldName, regionName, fee,
                        creationDate, terminated));
            }

            stmt.execute(String.format(
                    "UPDATE %s AS contract, %s AS rental "
                            + "SET contract.state = 'end', rental.current_contract = NULL "
                            + "WHERE contract.state = 'terminating'"
                            + " AND contract.prepayment = 0"
                            + " AND contract.id = rental.current_contract",
                    contractTableName, rentalTableName));

            stmt.execute(String.format(
                    "UPDATE %s SET state = 'effective',"
                            + " prepayment = prepayment - 1,"
                            + " expiration = ADDDATE(expiration, 7) "
                            + "WHERE state = 'terminating' AND 0 < prepayment",
                    contractTableName));
        }

        if (worklist.isEmpty()) {
            return;
        }

        processContractExpirationLater(worklist);
    }

    void processContractExpirationLater(Queue<TerminatedContract> worklist) {
        new BukkitRunnable() {
            @Override
            public void run() {
                long timeLimit = System.currentTimeMillis()
                        + maxTaskExecutionTime;
                do {
                    TerminatedContract contract = worklist.poll();
                    if (contract == null) {
                        return;
                    }

                    if (contract.terminated) {
                        try {
                            terminateContract(contract);
                        } catch (Throwable e) {
                            logger.warning(String.format(
                                    "error in terminating the contract on region %s in world %s",
                                    contract.worldName, contract.regionName));
                        }
                    } else {
                        pay(contract.worldName, contract.regionName,
                                contract.fee);
                    }
                } while (System.currentTimeMillis() < timeLimit);

                processContractExpirationLater(worklist);
            }
        }.runTask(plugin);
    }

    void pay(String worldName, String regionName, double fee) {
        World world = Bukkit.getWorld(worldName);
        RegionManager wgRegionManager = WorldGuardPlugin.inst()
                .getRegionManager(world);
        ProtectedRegion region = wgRegionManager.getRegion(regionName);
        DefaultDomain owners = region.getOwners();

        Set<UUID> uuids = owners.getUniqueIds();
        if (uuids.size() == 0) {
            return;
        }

        double paymentPerOwner = fee / uuids.size();
        for (UUID uuid : uuids) {
            economy.depositPlayer(Bukkit.getOfflinePlayer(uuid),
                    paymentPerOwner);
        }
    }

    void updateSign(RentalSign sign) {
        BlockState state = sign.getLocation().getBlock().getState();
        String[] lines = ((Sign) state).getLines();
        updateSign(sign.rental, lines);
        state.update();
    }

    void updateSign(Rental rental, String[] lines) {
        Contract contract = rental.currentContract;

        lines[1] = String.format(messages.getString("landRentalFee"),
                rental.fee);
        if (contract == null) {
            lines[2] = String.format(
                    messages.getString("landRentalMaxPrepayment"),
                    rental.maxPrepayment);
            lines[3] = messages
                    .getString("landRentalState_" + rental.state.toString());
        } else {
            Calendar endDate = Calendar.getInstance();
            try {
                endDate.setTime(dateFormat.parse(contract.expiration));
            } catch (ParseException e) {
                throw new AssertionError(String.format(
                        "invalid date format: %s", contract.expiration));
            }
            endDate.add(Calendar.DAY_OF_MONTH, -1);
            endDate.add(Calendar.DAY_OF_MONTH, contract.prepayment * 7);
            lines[2] = rental.state == RentalState.READY
                    ? String.format(
                            messages.getString(
                                    "landRentalEndDateAndPrepayment"),
                            endDate.get(Calendar.YEAR),
                            endDate.get(Calendar.MONTH) - Calendar.JANUARY + 1,
                            endDate.get(Calendar.DAY_OF_MONTH),
                            contract.prepayment)
                    : String.format(messages.getString("landRentalEndDate"),
                            endDate.get(Calendar.YEAR),
                            endDate.get(Calendar.MONTH) - Calendar.JANUARY + 1,
                            endDate.get(Calendar.DAY_OF_MONTH));
            lines[3] = playerTable.getNameForUniqueId(contract.tenant);
        }
    }
}
