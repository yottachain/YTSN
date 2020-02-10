package com.ytfs.service.cost;

import com.ytfs.service.SNSynchronizer;
import com.ytfs.service.dao.UserAccessor;
import com.ytfs.service.packet.bp.RelationShipSum;
import java.util.Map;
import org.apache.log4j.Logger;

public class SumRelationShip extends Thread {

    private static final Logger LOG = Logger.getLogger(SumRelationShip.class);

    private static SumRelationShip instance;

    public static synchronized void startUp() {
        if (instance == null) {
            instance = new SumRelationShip();
            instance.start();
        }
    }

    public static synchronized void shutdown() {
        if (instance != null) {
            instance.interrupt();
        }
    }

    private void sum() throws InterruptedException {
        Map<String, Long> map = UserAccessor.sumRelationship();
        RelationShipSum relsum = new RelationShipSum();
        relsum.setValue(map);
        SNSynchronizer.ayncRequest(relsum, -1, 1);
    }

    @Override
    public void run() {
        while (!this.isInterrupted()) {
            try {
                Thread.sleep(1000 * 60 * 15);
                sum();
                Thread.sleep(1000 * 60 * 15);
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
