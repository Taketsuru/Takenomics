package jp.dip.myuminecraft.takenomics;

import java.util.ArrayDeque;
import java.util.Queue;

import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;

public class JobQueue {

    JavaPlugin      plugin;
    Logger          logger;
    Queue<Runnable> requests = new ArrayDeque<Runnable>();
    BukkitRunnable  scheduledTask;

    public JobQueue(JavaPlugin plugin, Logger logger) {
        this.plugin = plugin;
        this.logger = logger;
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
            scheduledTask = new BukkitRunnable() {
                public void run() {
                    processAsyncTasks();
                }
            };
            scheduledTask.runTaskAsynchronously(plugin);
        }
    }
    
    void processAsyncTasks() {
        try {
            Runnable task;
            synchronized (this) {
                assert ! requests.isEmpty();
                task = requests.peek();
            }
    
            for (;;) {
                try {
                    task.run();
                } catch (Throwable t) {
                    logger.warning(t, "Failed to run a job queue task.");
                }
    
                synchronized (this) {
                    requests.poll();
                    if (requests.isEmpty()) {
                        return;
                    }
                    task = requests.peek();
                }            
            }
        } finally {
            scheduledTask = null;
        }
    }

}
