package com.ytfs.service.net;

import com.ytfs.service.packet.SerializationUtil;
import static com.ytfs.service.packet.ServiceErrorCode.INTERNAL_ERROR;
import com.ytfs.service.packet.ServiceException;
import com.ytfs.service.servlet.FromBPMsgDispatcher;
import com.ytfs.service.servlet.FromNodeMsgDispatcher;
import com.ytfs.service.servlet.FromUserMsgDispatcher;
import io.yottachain.nodemgmt.core.vo.Node;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import io.yottachain.p2phost.YottaP2P;
import io.yottachain.p2phost.core.exception.P2pHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.log4j.Logger;

public class P2PUtils {
    
    private static final Logger LOG = Logger.getLogger(P2PUtils.class);
    private static final List<String> CONNECTS = Collections.synchronizedList(new ArrayList());

    /**
     * 初始化P2P工具
     *
     * @param port
     * @param privateKey
     * @throws java.lang.Exception
     */
    public static void start(int port, String privateKey) throws Exception {
        YottaP2P.start(port, privateKey);
        LOG.info("P2P initialization completed, port " + port);
        LOG.info("Super NodeID:" + YottaP2P.id());
        String[] addrs = YottaP2P.addrs();
        for (int ii = 0; ii < addrs.length; ii++) {
            LOG.info("Super Node Addrs " + ii + ":" + addrs[ii]);
        }
    }
    
    public static void register() {
        YottaP2P.registerUserCallback(new FromUserMsgDispatcher());
        YottaP2P.registerBPNodeCallback(new FromBPMsgDispatcher());
        YottaP2P.registerNodeCallback(new FromNodeMsgDispatcher());
    }
    
    public static final int MSG_2BPU = 0;
    public static final int MSG_2BP = 1;
    public static final int MSG_2NODE = 2;
    
    public static Object requestBPU(Object obj, SuperNode node) throws ServiceException {
        return request(obj, node.getAddrs(), node.getNodeid(), MSG_2BPU);
    }
    
    public static Object requestBP(Object obj, SuperNode node) throws ServiceException {
        return request(obj, node.getAddrs(), node.getNodeid(), MSG_2BP);
    }
    
    public static Object requestNode(Object obj, Node node) throws ServiceException {
        return request(obj, node.getAddrs(), node.getNodeid(), MSG_2NODE);
    }
    
    public static Object request(Object obj, List<String> addr, String key, int type) throws ServiceException {
        if (!CONNECTS.contains(key)) {
            try {
                String[] strs = new String[addr.size()];
                strs = addr.toArray(strs);
                YottaP2P.connect(key, strs);
            } catch (P2pHostException ex) {
                throw new ServiceException(INTERNAL_ERROR, ex.getMessage());
            }
            CONNECTS.add(key);
        }
        byte[] data = SerializationUtil.serialize(obj);
        byte[] bs = null;
        try {                    //访问p2p网络
            switch (type) {
                case MSG_2BPU:
                    bs = YottaP2P.sendToBPUMsg(key, data);
                    break;
                case MSG_2BP:
                    bs = YottaP2P.sendToBPMsg(key, data);
                    break;
                default:
                    bs = YottaP2P.sendToNodeMsg(key, data);
                    break;
            }
        } catch (Throwable e) {
            LOG.error("INTERNAL_ERROR:", e);
            throw new ServiceException(INTERNAL_ERROR, e.getMessage());
        }
        Object res = SerializationUtil.deserialize(bs);
        if (res instanceof ServiceException) {
            throw (ServiceException) res;
        }
        return res;
    }

    /**
     * 销毁
     */
    public static void stop() {
        try {
            YottaP2P.close();
        } catch (P2pHostException ex) {
        }
    }
}
