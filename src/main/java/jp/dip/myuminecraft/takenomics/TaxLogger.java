package jp.dip.myuminecraft.takenomics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.Date;
import java.util.Deque;

import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;

public class TaxLogger {

    enum PrinterState {
        normal,
        shutdownRequested,
        shutdown
    }

    TaxLogger(JavaPlugin plugin) {
        this.plugin = plugin;

        FileConfiguration config = plugin.getConfig();

        if (! config.contains("log")) {
            return;
        }
            
        if (! config.isBoolean("log.enable")) {
            plugin.getLogger().warning(String.format("[%s] the value of 'log.enable' must be a boolean.",
                    plugin.getName()));
            return;
        }

        if (! config.getBoolean("log.enable")) {
            return;
        }

        if (! config.contains("log.path")) {
            plugin.getLogger().warning(String.format("[%s] the path name of the log is not configured.",
                    plugin.getName()));
            return;
        }
        
        String logname = config.getString("log.path");
        Path path = plugin.getDataFolder().toPath().resolve(logname);
        try {
            output = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            plugin.getLogger().warning(String.format("[%s] failed to open log file '%s'.",
                    plugin.getName(), logname));
            return;
        }

       printerState = PrinterState.normal;
        new BukkitRunnable() {
            public void run() {
                runPrinter();
            }
        }.runTaskAsynchronously(plugin);
    }

    public void disable() {
        shutdownPrinter();
        if (output != null) {
            try {
                output.close();
            } catch (IOException e) {
                warnIOError(e);
            }
            output = null;
        }
    }
    
    public synchronized void put(TaxRecord record) {
        if (output == null) {
            return;
        }
        
        boolean wakeup = queue.isEmpty();
        queue.addLast(record);
        if (wakeup) {
            notifyAll();
        }
    }
    
    void runPrinter() {
        for (;;) {
            TaxRecord record = fetchRecord();
            if (record == null) {
                break;
            }
            
            try {
                output.write(record.toString());
                output.newLine();
            } catch (IOException e) {
                warnIOError(e);
            }
        }
    }
    
    void warnIOError(IOException e) {
        plugin.getLogger().warning(String.format("[%s] failed to write to the log file.",
                plugin.getName()));        
    }

    synchronized void shutdownPrinter() {
        if (printerState == PrinterState.shutdown) {
            return;
        }
        
        printerState = PrinterState.shutdownRequested;
        notifyAll();

        while (printerState != PrinterState.shutdown) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
        }
    }
    
    synchronized TaxRecord fetchRecord() {
        while (queue.isEmpty()) {
            if (printerState == PrinterState.shutdownRequested) {
                printerState = PrinterState.shutdown;
                notifyAll();
                return null;
            }

            try {
                output.flush();
                wait();
            } catch (IOException e) {
                warnIOError(e);
            } catch (InterruptedException e) {
            }
        }

        return queue.removeFirst();
    }

    BufferedWriter   output;
    JavaPlugin       plugin;
    PrinterState     printerState = PrinterState.shutdown;
    Deque<TaxRecord> queue        = new ArrayDeque<TaxRecord>();
}
