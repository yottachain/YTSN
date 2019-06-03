package com.ytfs.service.servlet.bp;

import com.ytfs.common.conf.ServerConfig;
import static com.ytfs.common.ServiceErrorCode.INVALID_NODE_ID;
import com.ytfs.common.ServiceException;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.packet.AddDNIReq;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.List;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

public class PutDNIHandler extends Handler<AddDNIReq> {

    private static final Logger LOG = Logger.getLogger(PutDNIHandler.class);

    @Override
    public Object handle() throws Throwable {
        verify();
        putDNI(this.request.getDnis());
        return new VoidResp();
    }

    public static void putDNI(List<AddDNIReq.DNI> ls) {
        for (AddDNIReq.DNI dni : ls) {
            try {
                YottaNodeMgmt.addDNI(dni.getNodeid(), dni.getVHF());
            } catch (NodeMgmtException ne) {
                LOG.error("PutDNI " + dni.getNodeid() + "-[" + Hex.encodeHexString(dni.getVHF()) + "] ERR", ne);
            }
        }
    }

    private void verify() throws ServiceException {
        List<AddDNIReq.DNI> ls = this.request.getDnis();
        for (AddDNIReq.DNI dni : ls) {
            SuperNode sn = SuperNodeList.getNGRSuperNode(dni.getNodeid());
            if (sn.getId() != ServerConfig.superNodeID) {
                throw new ServiceException(INVALID_NODE_ID);
            }
        }
    }

}
