package jp.dip.myuminecraft.takenomics;

public class Logger {

    java.util.logging.Logger logger;

    Logger(java.util.logging.Logger logger) {
        this.logger = logger;
    }
    
    public void warning(String format, Object...args) {
        logger.warning(String.format(null, format, args));
    }

    public void info(String format, Object...args) {
        logger.info(String.format(null, format, args));
    }

}
