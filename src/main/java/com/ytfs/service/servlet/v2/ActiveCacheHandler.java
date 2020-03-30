package com.ytfs.service.servlet.v2;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.User;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.v2.ActiveCacheV2;
import com.ytfs.service.servlet.CacheAccessor;
import com.ytfs.service.servlet.Handler;

public class ActiveCacheHandler extends Handler<ActiveCacheV2> {

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser(request);
        if (user == null) {
            return new ServiceException(ServiceErrorCode.INVALID_SIGNATURE);
        }
        try {
            CacheAccessor.getUploadObjectCache(request.getVNU());
        } catch (Exception r) {
        }
        return new VoidResp();
    }

}
