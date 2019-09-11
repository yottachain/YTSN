package com.ytfs.service.servlet.user;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.User;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.bp.ActiveCache;
import com.ytfs.service.servlet.CacheAccessor;
import com.ytfs.service.servlet.Handler;

public class ActiveCacheHandler extends Handler<ActiveCache> {

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        if (user == null) {
            return new ServiceException(ServiceErrorCode.NEED_LOGIN);
        }
        try {
            CacheAccessor.getUploadObjectCache(request.getVNU());
        } catch (Exception r) {
        }
        return new VoidResp();
    }

}
