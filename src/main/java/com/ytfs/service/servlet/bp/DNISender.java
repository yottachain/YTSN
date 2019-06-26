package com.ytfs.service.servlet.bp;

import com.ytfs.common.GlobleThreadPool;
import com.ytfs.common.conf.ServerConfig;
import static com.ytfs.common.conf.ServerConfig.SENDDNITHREAD;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.packet.bp.AddDNIReq;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

public class DNISender implements Runnable {

    private static final Logger LOG = Logger.getLogger(DNISender.class);
    private static final ArrayBlockingQueue<DNISender> queue;

    static {
        int num = SENDDNITHREAD > 255 ? 255 : SENDDNITHREAD;
        num = num < 5 ? 5 : num;
        queue = new ArrayBlockingQueue(num);
        for (int ii = 0; ii < num; ii++) {
            queue.add(new DNISender());
        }
    }

    public static void startSender(byte[] VHF, int nid) {
        try {
            DNISender sender = queue.take();
            sender.nid = nid;
            sender.VHF = VHF;
            GlobleThreadPool.execute(sender);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private byte[] VHF;
    private int nid;

    @Override
    public void run() {
        try {
            SuperNode sn = SuperNodeList.getDNISuperNode(nid);
            AddDNIReq req = new AddDNIReq();
            req.setDni(VHF);
            req.setNodeid(nid);
            if (sn.getId() == ServerConfig.superNodeID) {
                YottaNodeMgmt.addDNI(nid, VHF);
            } else {
                P2PUtils.requestBP(req, sn);
            }
        } catch (Throwable r) {
            LOG.error("PutDNI " + nid + "-[" + Hex.encodeHexString(VHF) + "] ERR:" + r.getMessage());
        } finally {
            queue.add(this);
        }
    }
}
