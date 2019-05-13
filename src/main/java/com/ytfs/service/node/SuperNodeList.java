package com.ytfs.service.node;

import com.ytfs.service.ServiceWrapper;
import com.ytfs.client.UserConfig;
import com.ytfs.service.net.P2PUtils;
import com.ytfs.service.packet.ListSuperNodeReq;
import com.ytfs.service.packet.ListSuperNodeResp;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import org.apache.log4j.Logger;

public class SuperNodeList {

    static SuperNode[] superList = null;
    private static final Logger LOG = Logger.getLogger(SuperNodeList.class);

    /**
     * 客户端获取node列表
     *
     * @return SuperNode[]
     */
    private static SuperNode[] getSuperNodeList() {
        if (superList != null) {
            return superList;
        }
        synchronized (SuperNodeList.class) {
            if (superList == null) {
                try {
                    if (ServiceWrapper.isServer()) {
                        superList = NodeManager.getSuperNode();
                    } else {
                        ListSuperNodeReq req = new ListSuperNodeReq();
                        ListSuperNodeResp res = (ListSuperNodeResp) P2PUtils.requestBPU(req, UserConfig.superNode);
                        superList = res.getSuperList();
                    }
                } catch (Exception ex) {
                    LOG.error("", ex);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ex1) {
                    }
                }
            }
        }
        return superList;
    }

    /**
     * 获取数据块所属超级节点编号
     *
     * @param src
     * @return　0-32;
     */
    public static SuperNode getBlockSuperNode(byte[] src) {
        int value = (int) (((src[0] & 0xFF) << 24)
                | ((src[1] & 0xFF) << 16)
                | ((src[2] & 0xFF) << 8)
                | (src[3] & 0xFF));
        value = value & 0x0FFFF;
        SuperNode[] nodes = getSuperNodeList();
        int index = value % nodes.length;
        return nodes[index];
    }

    /**
     * 根据超级节点编号获取超级节点
     *
     * @param id
     * @return 0-32;
     */
    public static SuperNode getBlockSuperNode(int id) {
        SuperNode[] nodes = getSuperNodeList();
        return nodes[id];
    }

    /**
     * 获取管理该用户的超级节点
     *
     * @param userid
     * @return 0-32;
     */
    public static SuperNode getUserSuperNode(int userid) {
        SuperNode[] nodes = getSuperNodeList();
        int index = userid % nodes.length;
        return nodes[index];
    }

    /**
     * 获取管理该矿机的超级节点
     *
     * @param nodeid
     * @return 0-32;
     */
    public static SuperNode getNGRSuperNode(int nodeid) {
        SuperNode[] nodes = getSuperNodeList();
        int index = nodeid % nodes.length;
        return nodes[index];
    }

}
