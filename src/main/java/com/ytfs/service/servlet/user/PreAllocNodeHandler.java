package com.ytfs.service.servlet.user;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.node.NodeManager;
import com.ytfs.service.dao.User;
import com.ytfs.service.packet.user.PreAllocNode;
import com.ytfs.service.packet.user.PreAllocNodeReq;
import com.ytfs.service.packet.user.PreAllocNodeResp;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.Node;
import io.yottachain.ytcrypto.YTCrypto;
import io.yottachain.ytcrypto.core.exception.YTCryptoException;
import java.nio.charset.Charset;
import java.util.List;
import org.apache.log4j.Logger;

public class PreAllocNodeHandler extends Handler<PreAllocNodeReq> {

    private static final Logger LOG = Logger.getLogger(PreAllocNodeHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        if (user == null) {
            return new ServiceException(ServiceErrorCode.NEED_LOGIN);
        }
        int count = request.getCount() > 500 ? 500 : request.getCount();
        count = count < 100 ? 100 : count;
        PreAllocNodeResp resp = new PreAllocNodeResp();
        try {
            List<Node> nodes = NodeManager.preAllocNode(count);
            nodes.stream().map((node) -> new PreAllocNode(node)).filter((n) -> (resp.addNode(n))).forEach((n) -> {
                sign(n);
            });
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
