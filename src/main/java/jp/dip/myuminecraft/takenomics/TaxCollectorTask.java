package jp.dip.myuminecraft.takenomics;

import org.bukkit.scheduler.BukkitRunnable;

public class TaxCollectorTask extends BukkitRunnable {
    TaxOnSavingsCollector collector;

    TaxCollectorTask(TaxOnSavingsCollector collector) {
        this.collector = collector;
    }

    @Override
    public void run() {
        collector.collect();
    }
}
