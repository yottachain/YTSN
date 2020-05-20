package com.ytfs.service.servlet.node;

import com.ytfs.common.ServiceErrorCode;
import static com.ytfs.common.ServiceErrorCode.INVALID_NODE_ID;
import static com.ytfs.common.ServiceErrorCode.getErrMessage;
import com.ytfs.common.ServiceException;
import com.ytfs.common.node.NodeInfo;
import static com.ytfs.service.ServiceWrapper.REBUILDER_NODEID;
import static com.ytfs.service.ServiceWrapper.SPOTCHECK;
import com.ytfs.service.packet.StatusRepReq;
import com.ytfs.service.packet.StatusRepResp;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.servlet.Handler;
import com.ytfs.service.servlet.bp.NodeStatSync;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.Node;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

public class StatusRepHandler extends Handler<StatusRepReq> {

    static final String excludeAddrPrefix;

    static {
        String ss = System.getenv("NODEMGMT_EXCLUDEADDR");
        excludeAddrPrefix = ss == null ? "" : ss.trim();
    }

    private static final Logger LOG = Logger.getLogger(StatusRepHandler.class);

    @Override
    public Object handle() throws Throwable {
        int nodeid;
        try {
            nodeid = this.getNodeId();
        } catch (Throwable e) {
            LOG.error("Invalid node pubkey:" + this.getPublicKey() + ",ID:" + request.getId() + "," + e);
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        if (nodeid != request.getId()) {
            LOG.error("StatusRep Nodeid ERR:" + nodeid + "!=" + request.getId());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID);
        }
        if (nodeid == REBUILDER_NODEID) {
            return new VoidResp();
        }
        long l = System.currentTimeMillis();
        try {
            Node node = YottaNodeMgmt.updateNodeStatus(nodeid, request.getCpu(), request.getMemory(), request.getBandwidth(),
                    request.getMaxDataSpace(), request.getUsedSpace(), request.getAddrs(),
                    request.isRelay(), request.getVersion(), request.getRebuilding(),
                    request.getRealSpace(), request.getTx(), request.getRx(), request.getOther());
            StatusRepResp resp = new StatusRepResp();
            resp.setProductiveSpace(node.getProductiveSpace());
            List<String> ls = node.getAddrs();
            if (ls != null && !ls.isEmpty()) {
                resp.setRelayUrl(ls.get(0));
            }
            List<String> addrs = request.getAddrs();
            node.setAddrs(addrs);
            NodeStatSync.updateNode(node);
            LOG.debug("StatusRep Node:" + request.getId() + ",take times " + (System.currentTimeMillis() - l) + " ms");
            NodeInfo nodeinfo = this.getNode();
            List<String> newaddrs = checkPublicAddrs(addrs);
            nodeinfo.setAddr(newaddrs);
            if (SPOTCHECK) {
                SendSpotCheckTask.startUploadShard(nodeinfo);
            }
            return resp;
        } catch (NodeMgmtException e) {
            LOG.error("UpdateNodeStatus ERR:" + getErrMessage(e) + ",ID:" + request.getId() + ",take times " + (System.currentTimeMillis() - l) + " ms");
            return new ServiceException(INVALID_NODE_ID);
        }
    }

    public static List<String> checkPublicAddrs(List<String> addrs) {
        List<String> filteredAddrs = new ArrayList();
        addrs.forEach((addr) -> {
            if (addr.startsWith("/ip4/127.")
                    || addr.startsWith("/ip4/192.168.")
                    || addr.startsWith("/ip4/169.254.")
                    || addr.startsWith("/ip4/10.")
                    || addr.startsWith("/ip4/172.16.")
                    || addr.startsWith("/ip4/172.17.")
                    || addr.startsWith("/ip4/172.18.")
                    || addr.startsWith("/ip4/172.19.")
                    || addr.startsWith("/ip4/172.20.")
                    || addr.startsWith("/ip4/172.21.")
                    || addr.startsWith("/ip4/172.22.")
                    || addr.startsWith("/ip4/172.23.")
                    || addr.startsWith("/ip4/172.24.")
                    || addr.startsWith("/ip4/172.25.")
                    || addr.startsWith("/ip4/172.26.")
                    || addr.startsWith("/ip4/172.27.")
                    || addr.startsWith("/ip4/172.28.")
                    || addr.startsWith("/ip4/172.29.")
                    || addr.startsWith("/ip4/172.30.")
                    || addr.startsWith("/ip4/172.31.")
                    || addr.startsWith("/ip4/172.31.")
                    || addr.startsWith("/ip6/")
                    || addr.startsWith("/p2p-circuit/")) {
                if ((!excludeAddrPrefix.isEmpty()) && addr.startsWith(excludeAddrPrefix)) {
                    filteredAddrs.add(addr);
                }
            } else {
                filteredAddrs.add(addr);
            }
        });
        return filteredAddrs;
    }
}
