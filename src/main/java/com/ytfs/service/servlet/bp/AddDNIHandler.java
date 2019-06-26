package com.ytfs.service.servlet.bp;

import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.bp.AddDNIReq;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

public class AddDNIHandler extends Handler<AddDNIReq> {

    private static final Logger LOG = Logger.getLogger(AddDNIHandler.class);

    @Override
    public Object handle() throws Throwable {
        try {
            YottaNodeMgmt.addDNI(request.getNodeid(), request.getDni());
        } catch (NodeMgmtException ne) {
            LOG.error("PutDNI " + request.getNodeid() + "-[" + Hex.encodeHexString(request.getDni()) + "] ERR:" + ne.getMessage());
        }
        return new VoidResp();
    }

}
