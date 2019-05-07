package com.ytfs.service.node;

import com.mongodb.ServerAddress;
import com.ytfs.service.dao.MongoSource;
import io.jafka.jeos.util.Base58;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.Node;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

public class NodeManager {

    private static boolean started = false;
    private static final Logger LOG = Logger.getLogger(NodeManager.class);

    private synchronized static void start() throws NodeMgmtException {
        if (!started) {
            try {
                List<ServerAddress> addrs = MongoSource.getServerAddress();
                ServerAddress serverAddress = addrs.get(0);
                String addr = "mongodb://" + serverAddress.getHost() + ":" + serverAddress.getPort();
                YottaNodeMgmt.start(addr);
                started = true;
                LOG.info("NodeManager init...");
            } catch (NodeMgmtException ne) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                }
                throw ne;
            }
        }
    }

    private static SuperNode[] gettestSuperNode() {
        SuperNode[] sn = new SuperNode[1];
        SuperNode n = new SuperNode(0, null, null, null, null);
        List<String> addr = new ArrayList();
        addr.add("/p2p-circuit");
        addr.add("/ip4/127.0.0.1/tcp/9999");
        addr.add("/ip4/172.21.0.13/tcp/9999");
        n.setAddrs(addr);
        n.setNodeid("16Uiu2HAm4ejSpUiVYEYc2pCk7RUa3ScdswM6cXGwzTZziSKcAYwi");
        n.setId(0);
        n.setPrivkey("5JvCxXLSLzihWdXT7C9mtQkfLFHJZPdX1hxQo6su7dNt28mZ5W2");
        sn[0] = n;
        return sn;
    }

    /**
     * 获取超级节点列表,包括节点编号,加密或解签用公钥,接口地址
     *
     * @return Node[]
     * @throws io.yottachain.nodemgmt.core.exception.NodeMgmtException
     */
    public static SuperNode[] getSuperNode() throws NodeMgmtException {
        start();
        List<SuperNode> ls = YottaNodeMgmt.getSuperNodes();
        SuperNode[] sn = new SuperNode[ls.size()];
        for (SuperNode n : ls) {
            sn[n.getId()] = n;
        }
        if (sn.length == 0) {
            return gettestSuperNode();
        }
        return sn;
    }

    /**
     * 获取超级节点对应的私钥,一般超级节点对数据进行签名时需要
     *
     * @param id
     * @return byte[]
     * @throws io.yottachain.nodemgmt.core.exception.NodeMgmtException
     */
    public static byte[] getSuperNodePrivateKey(int id) throws NodeMgmtException {
        start();
        String str = YottaNodeMgmt.getSuperNodePrivateKey(id);
        return Base58.decode(str);
    }

    /**
     * 获取存储节点
     *
     * @param shardCount 根据某种算法分配shardCount个存储节点用来存储分片
     * @return
     * @throws io.yottachain.nodemgmt.core.exception.NodeMgmtException
     */
    public static Node[] getNode(int shardCount) throws NodeMgmtException {
        start();
        List<Node> ls = YottaNodeMgmt.allocNodes(shardCount);
        Node[] sn = new Node[ls.size()];
        return ls.toArray(sn);
    }

    /**
     * 获取节点
     *
     * @param nodeids
     * @return
     * @throws io.yottachain.nodemgmt.core.exception.NodeMgmtException
     */
    public static List<Node> getNode(List<Integer> nodeids) throws NodeMgmtException {
        start();
        List<Node> lss = YottaNodeMgmt.getNodes(nodeids);
        return lss;
    }

    public static int getNodeIDByPubKey(String key) throws NodeMgmtException {
        start();
        return YottaNodeMgmt.getNodeIDByPubKey(key);
    }

    public static int getSuperNodeIDByPubKey(String key) throws NodeMgmtException {
        start();
        return YottaNodeMgmt.getSuperNodeIDByPubKey(key);
    }

    /**
     * 节点吊线,需要惩罚
     *
     * @param nodeid
     */
    public static void punishNode(int nodeid) {
    }

    /**
     * 通报节点空间不足
     *
     * @param nodeid
     */
    public static void noSpace(int nodeid) {

    }

    /**
     * 向该存储节点对应的超级节点BPM发送消息,BPM记录该存储节点已经存储了VHF数据分片，
     * 相应增加该存储节点的已使用空间计数数据存储容量，但无需增加该存储节点的单位收益每周期收益
     *
     * @param nodeid
     * @param VHF
     */
    public static void recodeBPM(int nodeid, byte[] VHF) {

    }

}
