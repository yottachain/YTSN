package com.ytfs.service.servlet.v2;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.node.NodeManager;
import com.ytfs.service.dao.User;
import com.ytfs.service.packet.user.PreAllocNode;
import com.ytfs.service.packet.user.PreAllocNodeResp;
import com.ytfs.service.packet.v2.PreAllocNodeReqV2;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.Node;
import io.yottachain.ytcrypto.YTCrypto;
import io.yottachain.ytcrypto.core.exception.YTCryptoException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

public class PreAllocNodeHandler extends Handler<PreAllocNodeReqV2> {

    private static final Logger LOG = Logger.getLogger(PreAllocNodeHandler.class);
    private static Cache<Integer, PreAllocNodeResp> PreAlloc_CACHE;

    public synchronized static Cache<Integer, PreAllocNodeResp> getPreAllocCache() {
        if (PreAlloc_CACHE == null) {
            PreAlloc_CACHE = CacheBuilder.newBuilder()
                    .expireAfterWrite(60, TimeUnit.SECONDS)
                    .maximumSize(10000)
                    .build();
            LOG.info("Init PreAlloc_CACHE,MaxSize:10000");
        }
        return PreAlloc_CACHE;
    } 

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser(request);
        if (user == null) {
            return new ServiceException(ServiceErrorCode.INVALID_SIGNATURE);
        }
        int count = request.getCount() > 1000 ? 1000 : request.getCount();
        count = count < 100 ? 100 : count;
        PreAllocNodeResp resp =getPreAllocCache().getIfPresent(user.getUserID());
        if (resp!=null){
            LOG.info("User " + user.getUserID() + " AllocNodes OK,from cache." );
            return resp;  
        }
        resp = new PreAllocNodeResp();
        try {
            LOG.info("User " + user.getUserID() + " AllocNodes,count:" + count);
            List<Node> nodes = NodeManager.getNode(1000, request.getExcludes());
            for (Node node : nodes) {
                if (resp.getList().size() >= count) {
                    break;
                } else {
                    PreAllocNode n = new PreAllocNode(node);
                    if (resp.addNode(n)) {
                        sign(n);
                    }
                }
            }
            LOG.info("User " + user.getUserID() + " AllocNodes OK,return " + resp.getList().size());
            getPreAllocCache().put(user.getUserID(), resp);
            return resp;
        } catch (NodeMgmtException ex) {
            LOG.error("AllocNodes ERR:" + ex.getMessage());
            throw new ServiceException(ServiceErrorCode.SERVER_ERROR);
        }
    }

    private void sign(PreAllocNode node) {
        StringBuilder sb = new StringBuilder();
        sb.append(node.getId());
        node.getAddrs().stream().forEach((ip) -> {
            sb.append(ip);
        });
        sb.append(node.getPubkey());
        sb.append(node.getNodeid());
        sb.append(node.getTimestamp());
        byte[] data = sb.toString().getBytes(Charset.forName("UTF-8"));
        try {
            String sign = YTCrypto.sign(ServerConfig.privateKey, data);
            node.setSign(sign);
        } catch (YTCryptoException ex) {
            LOG.error("Sign ERR:" + ex.getMessage());
        }
    }
}
