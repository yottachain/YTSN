package com.ytfs.service.check;

import static com.ytfs.common.conf.ServerConfig.rebuildSpeed;
import static com.ytfs.common.conf.ServerConfig.rebuildTaskSize;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.packet.TaskDispatchList;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.vo.RebuildItem;
import io.yottachain.nodemgmt.core.vo.ShardCount;
import static java.lang.Thread.sleep;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.log4j.Logger;

public class QueryRebuildTask implements Runnable {

    private static final Logger LOG = Logger.getLogger(QueryRebuildTask.class);
    private static final int Max_Shard_Count = rebuildTaskSize;
    private final ShardCount shardCount;
    private final Map<Integer, QueryRebuildTask> taskMap;

    QueryRebuildTask(ShardCount shardCount, Map<Integer, QueryRebuildTask> taskMap) {
        this.shardCount = shardCount;
        this.taskMap = taskMap;
        this.taskMap.put(this.shardCount.getId(), this);
    }

    /*
    重建分发算法我先给一个简单的：
    假设故障节点有N个数据分片需要重建，
    每个重建节点一次最多重建M1（可配置参数）个数据分片，
    分配M2(可配置参数）个节点,，每个节点分配min(M1, N/M2)个数据分片；
    每个重建节点重建完自己的任务之后，再分配M1个数据分片给它，直到N个数据分片全部分配并且重建完
     */
    @Override
    public void run() {
        LOG.info("Node " + shardCount.getId() + " is starting to rebuild...");
        long index = 0;
        List<RebuildItem> items = new ArrayList();
        int snum = SuperNodeList.getSuperNodeCount();
        while (true) {
            try {
                long count = (index + 1) * Max_Shard_Count > shardCount.getCnt() ? shardCount.getCnt() % Max_Shard_Count : Max_Shard_Count;
                if (items.size() < snum) {
                    RebuildItem rebuildItem = YottaNodeMgmt.getRebuildItem(shardCount.getId(), index, count);
                    if (rebuildItem.getShards() == null || rebuildItem.getShards().isEmpty()) {
                        break;
                    }
                    items.add(rebuildItem);
                    index++;
                    pushTask(items);
                    items.clear();
                }
                if (Max_Shard_Count * index >= shardCount.getCnt()) {
                    break;
                }
                sleep(rebuildSpeed);
            } catch (Throwable t) {
                LOG.error("Get getRebuildItem ERR:" + t.getMessage());
                try {
                    sleep(15000);
                } catch (InterruptedException ex) {
                    break;
                }
            }
        }
        pushTask(items);
        taskMap.remove(shardCount.getId());
        LOG.info("Node " + shardCount.getId() + " has been reconstructed.");
    }

    private void pushTask(List<RebuildItem> itemlist) {
        List<Integer> nodeList = new ArrayList();
        itemlist.forEach((items) -> {
            nodeList.add(items.getNode().getId());
        });
        Map<Integer, TaskDispatchList> map = new HashMap();
        for (RebuildItem items : itemlist) {
            List<byte[]> ls = items.getShards();
            for (byte[] DNI : ls) {
                if (DNI == null || DNI.length != 27) {
                    LOG.warn("DNI Length Less than 27.");
                    continue;
                }
                int snnum = (int) DNI[0];
                TaskDispatchList list = map.get(snnum);
                if (list == null) {
                    list = new TaskDispatchList();
                    list.setNodeId(shardCount.getId());
                    int nid;
                    if (nodeList.isEmpty()) {
                        nid = items.getNode().getId();
                    } else {
                        nid = nodeList.remove(0);
                    }
                    list.setExecNodeId(nid);
                    list.setDNI(new ArrayList());
                    map.put(snnum, list);
                }
                list.addDNI(DNI);
            }
        }
        Set<Entry<Integer, TaskDispatchList>> set = map.entrySet();
        set.stream().forEach((ent) -> {
            SendRebuildTask.startRemoteSender(ent.getKey(), ent.getValue());
            LOG.info("Send Task OK,count:" + ent.getValue().getDNI().size() + ",NodeId:"
                    + ent.getValue().getNodeId() + ",ExecNodeId:" + ent.getValue().getExecNodeId());
        });
    }
}
