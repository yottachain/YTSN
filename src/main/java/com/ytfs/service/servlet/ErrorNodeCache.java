package com.ytfs.service.servlet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class ErrorNodeCache {

    private static final long EXPIRED_TIME = 1000 * 180;
    private static final Logger LOG = Logger.getLogger(ErrorNodeCache.class);

    private static final Map<Integer, Long> errIds = new ConcurrentHashMap<>();

    private static Thread instance;

    public static synchronized void startUp() {
        if (instance == null) {
            instance = new Thread() {
                @Override
                public void run() {
                    while (!this.isInterrupted()) {
                        try {
                            sleep(15000);
                            List<Map.Entry<Integer, Long>> ents = new ArrayList(errIds.entrySet());
                            for (Map.Entry<Integer, Long> ent : ents) {
                                if (System.currentTimeMillis() - ent.getValue() > EXPIRED_TIME) {
                                    errIds.remove(ent.getKey());
                                }
                            }
                        } catch (InterruptedException r) {
                            break;
                        } catch (Throwable r) {
                        }
                    }
                }
            };
            instance.start();
        }
    }

    public static synchronized void shutdown() {
        if (instance != null) {
            instance.interrupt();
        }
    }

    public static void addErrorNode(List<Integer> errid) {
        errid.stream().forEach((id) -> {
            errIds.put(id, System.currentTimeMillis());
        });
    }

    public static int[] getErrorIds() {
        List<Integer> idlist = new ArrayList(errIds.keySet());
        int[] ids = new int[idlist.size()];
        if (ids.length > 300) {
            LOG.warn("Error ids count exceed 300.");
        }
        for (int ii = 0; ii < ids.length && ii < 300; ii++) {
            ids[ii] = idlist.get(ii);
        }
        return ids;
    }
}
