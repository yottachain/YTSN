package com.ytfs.service.servlet.user;

import com.ytfs.common.GlobleThreadPool;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.ServiceException;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.packet.AddDNIReq;
import com.ytfs.service.packet.AddDNIReq.DNI;
import com.ytfs.service.servlet.bp.PutDNIHandler;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.Binary;

public class SendShard2SNM implements Runnable {

    private static final Logger LOG = Logger.getLogger(SendShard2SNM.class);

    public static void sendShard2SNM(List<Document> ls) throws InterruptedException {
        SendShard2SNM sender = new SendShard2SNM();
        sender.docs = ls;
        GlobleThreadPool.execute(sender);
    }

    private List<Document> docs;

    @Override
    public void run() {
        try {
            Map<SuperNode, AddDNIReq> map = makeRequest(docs);
            Set<Map.Entry<SuperNode, AddDNIReq>> set = map.entrySet();
            for (Map.Entry<SuperNode, AddDNIReq> ent : set) {
                try {
                    saveDNICall(ent.getValue(), ent.getKey());
                } catch (ServiceException se) {
                    LOG.error("Call PutDNI " + ent.getKey().getNodeid() + " ERR!", se);
                    printErr(ent.getValue());
                }
            }
        } catch (Throwable t) {
            LOG.error("Unkown ERR!", t);
        }
    }

    private void printErr(AddDNIReq req) {
        List<DNI> ls = req.getDnis();
        for (AddDNIReq.DNI dni : ls) {
            LOG.error("ERR List " + dni.getNodeid() + "-[" + Hex.encodeHexString(dni.getVHF()) + "]");
        }
    }

    private Map<SuperNode, AddDNIReq> makeRequest(List<Document> ls) {
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

    private void saveDNICall(AddDNIReq req, SuperNode sn) throws ServiceException {
        if (sn.getId() == ServerConfig.superNodeID) {
            PutDNIHandler.putDNI(req.getDnis());
        } else {
            P2PUtils.requestBP(req, sn);
        }
    }

}
