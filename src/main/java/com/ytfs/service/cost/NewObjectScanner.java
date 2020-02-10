package com.ytfs.service.cost;

import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.eos.EOSClient;
import com.ytfs.service.dao.CacheBaseAccessor;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;

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

    private void exec(Document doc) {
        ObjectId _id = doc.getObjectId("_id");
        long usedspace = doc.getLong("usedSpace");
        String username = doc.getString("username");
        int step = doc.getInteger("step");
        int userid = doc.getInteger("userid");
        if (step == 0) {
            try {
                EOSClient.addUsedSpace(usedspace, username);
                LOG.info("User " + userid + " add usedSpace:" + usedspace);
            } catch (Throwable e) {
                CacheBaseAccessor.addNewObject(_id, usedspace, userid, username, 0);
                LOG.error("Add usedSpace ERR:" + e.getMessage());
                try {
                    Thread.sleep(1000 * 60 * 3);
                } catch (InterruptedException ex) {
                }
                return;
            }
        }
        long firstCost = ServerConfig.unitFirstCost * usedspace / ServerConfig.unitSpace;
        try {
            EOSClient.deductHDD(firstCost, username);
            LOG.info("User " + userid + " sub Balance:" + firstCost);
        } catch (Throwable e) {
            CacheBaseAccessor.addNewObject(_id, usedspace, userid, username, 1);
            LOG.error("Sub Balance ERR:" + e.getMessage());
            try {
                Thread.sleep(1000 * 60 * 3);
            } catch (InterruptedException ex) {
            }
        }
    }

    @Override
    public void run() {
        while (!this.isInterrupted()) {
            try {
                Document doc = CacheBaseAccessor.findOneNewObject();
                if (doc != null) {
                    exec(doc);
                } else {
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
