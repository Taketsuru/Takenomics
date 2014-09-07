package jp.dip.myuminecraft.tax;

import org.bukkit.scheduler.BukkitRunnable;

public class TaxCollectorTask extends BukkitRunnable {
    TaxCollector collector;

    TaxCollectorTask(TaxCollector collector) {
        this.collector = collector;
    }

    @Override
    public void run() {
        collector.collect();
    }
}
