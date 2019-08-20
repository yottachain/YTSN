package com.ytfs.service.check;

import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.RebuildItem;
import io.yottachain.nodemgmt.core.vo.ShardCount;
import static java.lang.Thread.sleep;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class QueryRebuildTask implements Runnable {

    private static final Logger LOG = Logger.getLogger(QueryRebuildTask.class);
    private static final int Max_Shard_Count = 1000;
    private final ShardCount shardCount;
    private final Map<Integer, QueryRebuildTask> taskMap;

    QueryRebuildTask(ShardCount shardCount, Map<Integer, QueryRebuildTask> taskMap) {
        this.shardCount = shardCount;
        this.taskMap = taskMap;
        this.taskMap.put(this.shardCount.getId(), this);
    }

    @Override
    public void run() {
        LOG.info("Node " + shardCount.getId() + " is starting to rebuild...");
        long index = 0;
        while (true) {
            try {
                long count = (index + 1) * Max_Shard_Count > shardCount.getCnt() ? shardCount.getCnt() % Max_Shard_Count : Max_Shard_Count;
                queryTask(index, count);
                index++;
                if (Max_Shard_Count * index >= shardCount.getCnt()) {
                    break;
                }
            } catch (Throwable t) {
                LOG.error("Get getRebuildItem ERR:" + t.getMessage());
                try {
                    sleep(15000);
                } catch (InterruptedException ex) {
                    break;
                }
            }
        }
        taskMap.remove(shardCount.getId());
        LOG.info("Node " + shardCount.getId() + " has been reconstructed.");
    }

    private void queryTask(long index, long count) throws NodeMgmtException {
        RebuildItem items = YottaNodeMgmt.getRebuildItem(shardCount.getId(), index, count);
        List<byte[]> ls = items.getShards();
        if (ls != null) {
            for (byte[] DNI : ls) {
                SendRebuildTask.startSender(DNI, shardCount.getId(), items.getNode());
            }
        }
    }
}
