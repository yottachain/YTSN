package com.ytfs.service.servlet.user;

import com.ytfs.service.GlobleThreadPool;
import com.ytfs.service.ServerConfig;
import com.ytfs.service.ServiceException;
import com.ytfs.service.net.P2PUtils;
import com.ytfs.service.node.SuperNodeList;
import com.ytfs.service.packet.AddDNIReq;
import com.ytfs.service.packet.AddDNIReq.DNI;
import com.ytfs.service.packet.QueryObjectMetaReq;
import com.ytfs.service.packet.QueryObjectMetaResp;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.bson.types.Binary;

public class SendShard2SNM implements Runnable {

    public static void sendShard2SNM(List<Document> ls) throws InterruptedException {
        SendShard2SNM sender = new SendShard2SNM();
        sender.mapReq = makeRequest(ls);
        GlobleThreadPool.execute(sender);
    }

    private Map<SuperNode, AddDNIReq> mapReq;

    private static Map<SuperNode, AddDNIReq> makeRequest(List<Document> ls) {
        Map<SuperNode, AddDNIReq> map = new HashMap();
        for (Document doc : ls) {
            DNI dni = new DNI();
            dni.setNodeid(doc.getInteger("nodeId"));
            dni.setVHF(((Binary) doc.get("VHF")).getData());
            SuperNode sn = SuperNodeList.getNGRSuperNode(dni.getNodeid());
            AddDNIReq req = map.get(sn);
            if (req == null) {
                req = new AddDNIReq();
                map.put(sn, req);
            }
            req.addDNI(dni);
        }
        return map;
    }

    @Override
    public void run() {
        try {

        } finally {

        }
    }

    public static void saveDNICall(AddDNIReq req, SuperNode sn) throws ServiceException {
        if (sn.getId() == ServerConfig.superNodeID) {
            //resp = queryObjectMeta(req);
        } else {
           // resp = (QueryObjectMetaResp) P2PUtils.requestBP(req, node);
        }
    }

}
