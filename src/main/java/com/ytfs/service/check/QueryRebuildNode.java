package com.ytfs.service.check;

import com.ytfs.common.GlobleThreadPool;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.vo.ShardCount;
import static java.lang.Thread.sleep;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class QueryRebuildNode extends Thread {

    private static final Logger LOG = Logger.getLogger(QueryRebuildNode.class);

    private boolean exit = false;
    private static QueryRebuildNode instance;
    private static QueryRebuildTaskQueue queryRebuildTaskQueue;

    public static synchronized void startUp() {
        if (instance == null) {
            instance = new QueryRebuildNode();
            instance.start();
        }
        if (queryRebuildTaskQueue == null) {
            queryRebuildTaskQueue = new QueryRebuildTaskQueue();
            queryRebuildTaskQueue.start();
        }
    }

    public static synchronized void shutdown() {
        if (instance != null) {
            instance.exit = true;
            instance.interrupt();
            instance = null;
        }
        if (queryRebuildTaskQueue != null) {
            queryRebuildTaskQueue.interrupt();
            queryRebuildTaskQueue = null;
        }
    }

    private final Map<Integer, QueryRebuildTask> taskMap = new ConcurrentHashMap<>();

    @Override
    public void run() {
        LOG.info("SendRebuildTask distributor startup...");
        List<ShardCount> sc = null;
        while (!exit) {
            try {
                long time = System.currentTimeMillis();
                long min = time % 3600000L;
                //long min = time % 600000L;
                try {
                    if (min < 60000 * 3) {
                        sc = YottaNodeMgmt.getInvalidNodes();
                        LOG.info("Query returns " + (sc == null ? 0 : sc.size()) + " tasks.");
                    }
                } catch (Throwable t) {
                    LOG.error("Get RebuildTask ERR:" + t);
                    sleep(30000);
                    continue;
                }
                if (sc != null) {//分发
                    for (ShardCount shardcount : sc) {
                        if (taskMap.containsKey(shardcount.getId())) {
                            LOG.info("Node " + shardcount.getId() + " is already rebuilding.");
                        } else {
                            QueryRebuildTask sr = new QueryRebuildTask(shardcount, taskMap);
                            GlobleThreadPool.execute(sr);
                        }
                    }
                    sc.clear();
                    sleep(60000 * 3);
                }
                sleep(30000);
            } catch (InterruptedException ex) {
                break;
            } catch (Throwable ne) {
                try {
                    sleep(15000);
                } catch (InterruptedException ex) {
                    break;
                }
            }
        }
    }

}
