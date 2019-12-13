package com.ytfs.service.servlet.bp;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.BlockAccessor;
import com.ytfs.service.packet.LongResp;
import com.ytfs.service.packet.bp.GetBlockUsedSpace;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import java.util.List;
import org.apache.log4j.Logger;

public class BlockUsedSpaceHandler extends Handler<GetBlockUsedSpace> {

    private static final Logger LOG = Logger.getLogger(BlockUsedSpaceHandler.class);

    public static long getBlockUsedSpaceCall(List<Long> ids) {
        return BlockAccessor.getUsedSpace(ids);
    }

    @Override
    public Object handle() throws Throwable {
        try {
            getSuperNodeId();
        } catch (NodeMgmtException e) {
            LOG.error("Invalid super node pubkey:" + this.getPublicKey());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        long usedspace = getBlockUsedSpaceCall(this.request.getId());
        LongResp lp = new LongResp();
        lp.setValue(usedspace);
        return lp;
    }

}
