package com.ytfs.service.servlet.bp;

import com.ytfs.common.conf.ServerConfig;
import com.ytfs.service.SNSynchronizer;
import com.ytfs.service.packet.bp.NodeSyncReq;
import io.yottachain.nodemgmt.core.vo.Node;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class NodeStatSync extends Thread {

    private static final Logger LOG = Logger.getLogger(NodeStatSync.class);
    private static final int sleeptimes = 1000 * 60;
    private static final Map<Integer, Node> nodestats = new ConcurrentHashMap<>();

    public static void updateNode(Node node) {
        nodestats.put(node.getId(), node);
    }

    private static Thread me;

    public static void startup() {
        if (me == null) {
            me = new NodeStatSync();
            me.start();
        }
    }

    public static void terminate() {
        if (me != null) {
            me.interrupt();
            try {
                me.join();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            me = null;
        }
    }

    @Override
    public void run() {
        LOG.info("Node stat sync thread startup....");
        while (!this.isInterrupted()) {
            try {
                sync();
                sleep(sleeptimes);
            } catch (InterruptedException e) {
                break;
            } catch (Throwable e) {
                LOG.error("Node stat sync ERR:" + e.getMessage());
            }
        }
    }

    private void sync() throws InterruptedException {
        List<Node> nodes = new ArrayList(nodestats.values());
        if (!nodes.isEmpty()) {
            NodeSyncReq req = new NodeSyncReq();
            req.setNode(nodes);
            SNSynchronizer.ayncRequest(req, ServerConfig.superNodeID);
            LOG.debug("Sync Node STAT,count:" + nodes.size());
        }
    }

}
