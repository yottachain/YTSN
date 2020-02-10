package com.ytfs.service.servlet.bp;

import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.node.SuperNodeList;
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
                if (SuperNodeList.isActive()) {
                    sync();
                }
                sleep(sleeptimes);
            } catch (InterruptedException e) {
                break;
            } catch (Throwable e) {
                LOG.error("Node stat sync ERR:" + e.getMessage());
            }
        }
    }

    private void sync() throws InterruptedException {
        List<Node> coll = new ArrayList(nodestats.values());
        List<Node> nodes = new ArrayList();
        for (Node node : coll) {
            if (System.currentTimeMillis() - (node.getTimestamp() * 1000L) < 1000 * 60 * 4) {
                nodes.add(node);
            } else {
                nodestats.remove(node.getId());
            }
        }
        if (!nodes.isEmpty()) {
            NodeSyncReq req = new NodeSyncReq();
            req.setNode(nodes);
            SNSynchronizer.ayncRequest(req, ServerConfig.superNodeID, 1);
            LOG.debug("Sync Node STAT,count:" + nodes.size());
        }
    }

}
