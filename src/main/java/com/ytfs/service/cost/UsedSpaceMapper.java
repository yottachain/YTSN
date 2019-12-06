package com.ytfs.service.cost;

import com.ytfs.common.GlobleThreadPool;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.packet.LongResp;
import com.ytfs.service.packet.bp.GetBlockUsedSpace;
import com.ytfs.service.servlet.bp.BlockUsedSpaceHandler;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.log4j.Logger;

public class UsedSpaceMapper implements Runnable {

    private static final Logger LOG = Logger.getLogger(UsedSpaceMapper.class);
    private static ArrayBlockingQueue<UsedSpaceMapper> queue = null;

    private static synchronized ArrayBlockingQueue<UsedSpaceMapper> getQueue() {
        if (queue == null) {
            int count = SuperNodeList.getSuperNodeCount() * 5;
            queue = new ArrayBlockingQueue(count);
            for (int ii = 0; ii < count; ii++) {
                queue.add(new UsedSpaceMapper());
            }
        }
        return queue;
    }

    static void startUploadShard(List<Long> ids, int snId, UserFileIterator it) {
        try {
            UsedSpaceMapper getBlockUsedSpace = getQueue().take();
            getBlockUsedSpace.ids = ids;
            getBlockUsedSpace.snId = snId;
            getBlockUsedSpace.it = it;
            GlobleThreadPool.execute(getBlockUsedSpace);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private List<Long> ids;
    private int snId;
    private UserFileIterator it;

    @Override
    public void run() {
        while (true) {
            try {
                if (snId == ServerConfig.superNodeID) {
                    long usedSpace = BlockUsedSpaceHandler.getBlockUsedSpaceCall(ids);
                    it.addUsedSpace(usedSpace);
                } else {
                    GetBlockUsedSpace us = new GetBlockUsedSpace();
                    us.setId(ids);
                    SuperNode sn = SuperNodeList.getSuperNode(snId);
                    LongResp resp = (LongResp) P2PUtils.requestBPU(us, sn);
                    it.addUsedSpace(resp.getValue());
                }
                break;
            } catch (Throwable t) {
                LOG.error("ERR:" + t.getMessage());
                try {
                    Thread.sleep(1000 * 15);
                } catch (InterruptedException ex) {
                }
            }
        }
        getQueue().add(this);
    }

}
