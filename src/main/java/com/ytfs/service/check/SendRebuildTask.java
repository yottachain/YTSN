package com.ytfs.service.check;

import com.ytfs.common.GlobleThreadPool;
import static com.ytfs.common.conf.ServerConfig.REBULIDTHREAD;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.packet.TaskDispatchReq;
import com.ytfs.service.servlet.bp.TaskDispatchHandler;
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

    /**
     * 
     * @param DNI
     * @param nid 需重建节点
     * @param node 目标节点
     */
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
            try {
                int snnum = (int) DNI[0];
                TaskDispatchReq req = new TaskDispatchReq();
                req.setDNI(DNI);
                req.setNodeId(nodeid);
                req.setExecNodeId(node.getId());
                SuperNode sn = SuperNodeList.getSuperNode(snnum);
                TaskDispatchHandler.taskDispatchCall(req, sn);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Query rebuild task " + Base58.encode(DNI));
                }
            } catch (Throwable r) {
                LOG.error("Send rebuild task " + Base58.encode(DNI) + " ERR:" + r.getMessage());
            }
        } finally {
            queue.add(this);
        }
    }
}
