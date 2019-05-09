package com.ytfs.service.servlet;

import com.ytfs.service.dao.ObjectAccessor;
import org.apache.log4j.Logger;

public class NewObjectScanner extends Thread {

    private static final Logger LOG = Logger.getLogger(NewObjectScanner.class);

    @Override
    public void run() {
        while (!this.isInterrupted()) {
            try {
                boolean b = ObjectAccessor.listNewObject();
                if (b) {
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

    public void close() {
        this.interrupt();
    }

}
