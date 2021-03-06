package com.ytfs.service.servlet.node;

import com.ytfs.common.GlobleThreadPool;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.NodeInfo;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.packet.SpotCheckTaskList;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.SpotCheckList;
import io.yottachain.nodemgmt.core.vo.SpotCheckTask;
import io.yottachain.p2phost.utils.Base58;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

public class SendSpotCheckTask implements Runnable {
    
    private static final Logger LOG = Logger.getLogger(SendSpotCheckTask.class);
    
    private static ArrayBlockingQueue<SendSpotCheckTask> queue = null;
    
    private static synchronized ArrayBlockingQueue<SendSpotCheckTask> getQueue() {
        int num = SuperNodeList.getSuperNodeCount() * 2;
        if (queue == null) {
            queue = new ArrayBlockingQueue(num);
            for (int ii = 0; ii < num; ii++) {
                queue.add(new SendSpotCheckTask());
            }
        }
        return queue;
    }
    
    static void startUploadShard(NodeInfo nodeinfo) throws InterruptedException, NodeMgmtException {
        boolean bool = YottaNodeMgmt.spotcheckSelected();
        //LOG.info("Node " + nodeinfo.getId() + " spotcheck select return " + bool);
        if (bool) {
            SendSpotCheckTask task = getQueue().poll();
            if (task == null) {
                SendSpotCheckTask ct = new SendSpotCheckTask();
                ct.nodeinfo = nodeinfo;
                ct.execute();
            } else {
                task.nodeinfo = nodeinfo;
                GlobleThreadPool.execute(task);
            }
        }
    }
    
    private NodeInfo nodeinfo;
    
    private void execute() {
        List<SpotCheckList> sc;
        try {
            sc = YottaNodeMgmt.getSpotCheckList();
        } catch (Throwable e) {
            LOG.error("Get spotcheck list ERR:" + e.getMessage());
            return;
        }
        if (sc == null || sc.isEmpty()) {
            LOG.error("Get spotcheck list return empty.");
            return;
        }
        SpotCheckTaskList task = doTask(sc.get(0));
        try {
            P2PUtils.requestNode(task, nodeinfo.getPeerId(), nodeinfo.getId());
            LOG.info("Send task [" + task.getTaskId() + "] to " + nodeinfo.getId() + " OK.");
        } catch (Throwable e) {
            try {
                P2PUtils.requestNode(task, nodeinfo.getNode());
                LOG.info("Send task [" + task.getTaskId() + "] to " + nodeinfo.getId() + " OK.");
            } catch (Throwable ex1) {
                LOG.error("Send task [" + task.getTaskId() + "] to " + nodeinfo.getId() + " ERR:" + e.getMessage());
            }
        }
    }
    
    private SpotCheckTaskList doTask(SpotCheckList scheck) {
        SpotCheckTaskList mytask = new SpotCheckTaskList();
        mytask.setSnid(ServerConfig.superNodeID);
        mytask.setTaskId(scheck.getTaskID());
        mytask.setTaskList(new ArrayList());
        List<SpotCheckTask> ls = scheck.getTaskList();
        ls.stream().map((st) -> {
            com.ytfs.service.packet.SpotCheckTask myst = new com.ytfs.service.packet.SpotCheckTask();
            myst.setId(st.getId());
            myst.setNodeId(st.getNodeID());
            myst.setAddr(st.getAddr());
            byte[] vni = Base64.decodeBase64(st.getVni());
            if (vni.length > 16) {
                byte[] VHF = new byte[16];
                System.arraycopy(vni, vni.length - 16, VHF, 0, 16);
                myst.setVHF(VHF);
            } else {
                myst.setVHF(vni);
            }
            return myst;
        }).forEach((myst) -> {
            LOG.debug("Check VHF [" + Base58.encode(myst.getVHF()) + "] at " + myst.getId());
            mytask.getTaskList().add(myst);
        });
        return mytask;
    }
    
    @Override
    public void run() {
        try {
            execute();
        } finally {
            getQueue().add(this);
        }
    }
    
}
