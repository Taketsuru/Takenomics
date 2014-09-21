package jp.dip.myuminecraft.takenomics;

import java.util.ArrayDeque;
import java.util.Queue;

import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;

public class JobQueue {

    JavaPlugin      plugin;
    Queue<Runnable> requests = new ArrayDeque<Runnable>();
    BukkitRunnable  scheduledTask;

    public JobQueue(JavaPlugin plugin) {
        this.plugin = plugin;
    }

    public synchronized void drain() {
        while (scheduledTask != null) {
            try {
                wait();
            } catch (InterruptedException ie) {
            }
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

}
