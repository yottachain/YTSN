package com.ytfs.service.cost;

import com.ytfs.service.dao.CacheBaseAccessor;
import org.apache.log4j.Logger;

public class NewObjectScanner extends Thread {

    private static final Logger LOG = Logger.getLogger(NewObjectScanner.class);

    private static NewObjectScanner instance;

    public static synchronized void startUp() {
        if (instance == null) {
            instance = new NewObjectScanner();
            instance.start();
        }
    }

    public static synchronized void shutdown() {
        if (instance != null) {
            instance.interrupt();
        }
    }

    @Override
    public void run() {
        while (!this.isInterrupted()) {
            try {
                boolean b =true;// CacheBaseAccessor.listNewObject();
                if (!b) {
                    Thread.sleep(1000 * 60 * 10);
                }
            } catch (InterruptedException ex) {
                break;
            } catch (Throwable e) {
                LOG.error("", e);
                try {
                    Thread.sleep(1000 * 15);
                } catch (InterruptedException ex) {
                    break;
                }
            }
        }

    }

}
