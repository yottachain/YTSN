package com.ytfs.service.check;

import com.ytfs.common.GlobleThreadPool;
import static com.ytfs.common.conf.ServerConfig.REBULIDTHREAD;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.packet.TaskQueryReq;
import com.ytfs.service.servlet.bp.TaskQueryHandler;
import io.jafka.jeos.util.Base58;
import io.yottachain.nodemgmt.core.vo.Node;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.log4j.Logger;

public class SendRebuildTask implements Runnable {

    private static final Logger LOG = Logger.getLogger(SendRebuildTask.class);

    private static final ArrayBlockingQueue<SendRebuildTask> queue;

    static {
        int num = REBULIDTHREAD > 255 ? 255 : REBULIDTHREAD;
        num = num < 5 ? 5 : num;
        queue = new ArrayBlockingQueue(num);
        for (int ii = 0; ii < num; ii++) {
            queue.add(new SendRebuildTask());
        }
    }

    public static void startSender(byte[] DNI, int nid, Node node) {
        try {
            SendRebuildTask sender = queue.take();
            sender.nodeid = nid;
            sender.DNI = DNI;
            sender.node = node;
            GlobleThreadPool.execute(sender);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
    private byte[] DNI;
    private Node node;
    private int nodeid;

    @Override
    public void run() {
        try {
            if (DNI == null || DNI.length != 42) {
                LOG.warn("DNI Length Less than 42.");
                return;
            }
            Object task = null;
            try {
                int snnum = (int) DNI[0];
                TaskQueryReq req = new TaskQueryReq();
                req.setDNI(DNI);
                req.setNodeId(nodeid);
                SuperNode sn = SuperNodeList.getSuperNode(snnum);
                task = TaskQueryHandler.taskQueryCall(req, sn);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Query rebuild task " + Base58.encode(DNI));
                }
            } catch (Throwable r) {
                LOG.error("Query rebuild task " + Base58.encode(DNI) + " ERR:" + r.getMessage());
            }
            if (task != null) {
                try {
                    P2PUtils.requestNode(task, node);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Send rebuild task " + Base58.encode(DNI) + " to " + node.getId());
                    }
                } catch (Throwable ex) {
                    LOG.error("Send rebuild task " + Base58.encode(DNI) + " ERR:" + ex.getMessage());
                }
            }
        } finally {
            queue.add(this);
        }
    }
}
