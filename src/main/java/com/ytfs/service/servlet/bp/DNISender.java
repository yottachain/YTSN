package com.ytfs.service.servlet.bp;

import com.ytfs.common.ServiceException;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.dao.CacheBaseAccessor;
import com.ytfs.service.packet.bp.UpdateDNIMutiReq;
import com.ytfs.service.packet.bp.UpdateDNIReq;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

public class DNISender implements Runnable {

    private static final Logger LOG = Logger.getLogger(DNISender.class);

    private final SuperNode superNode;
    private final List<Document> ls;
    private final List<DNISender> sendList;

    DNISender(SuperNode superNode, List<Document> ls, List<DNISender> sendList) {
        this.superNode = superNode;
        this.ls = ls;
        this.sendList = sendList;
    }

    @Override
    public void run() {
        try {
            List<UpdateDNIReq> list = new ArrayList();
            ObjectId[] ids = new ObjectId[list.size()];
            int ii = 0;
            for (Document doc : ls) {
                ids[ii++] = doc.getObjectId("_id");
                UpdateDNIReq req = new UpdateDNIReq();
                req.setNodeid(doc.getInteger("nodeId"));
                req.setDelete(doc.getBoolean("delete"));
                req.setDni(((Binary) doc.get("vhf")).getData());
            }
            send(list);
            CacheBaseAccessor.deleteDNI(ids);
        } catch (Throwable t) {
            LOG.error("ERR:" + t.getMessage());
        } finally {
            synchronized (sendList) {
                sendList.remove(this);
                sendList.notify();
            }
        }
    }

    private void send(List<UpdateDNIReq> list) throws ServiceException {
        UpdateDNIMutiReq mreq = new UpdateDNIMutiReq();
        mreq.setList(list);
        P2PUtils.requestBP(mreq, superNode);
        LOG.info("Send DNI OK,count:" + list.size());
    }

}
