package com.ytfs.service.servlet.bp;

import com.ytfs.service.dao.BlockAccessor;
import com.ytfs.service.packet.LongResp;
import com.ytfs.service.packet.bp.GetBlockUsedSpace;
import com.ytfs.service.servlet.Handler;
import java.util.List;

public class BlockUsedSpaceHandler extends Handler<GetBlockUsedSpace> {

    public static long getBlockUsedSpaceCall(List<Long> ids) {
        return BlockAccessor.getUsedSpace(ids);
    }

    @Override
    public Object handle() throws Throwable {
        long usedspace = getBlockUsedSpaceCall(this.request.getId());
        LongResp lp = new LongResp();
        lp.setValue(usedspace);
        return lp;
    }

}
