package com.ytfs.service.servlet.user;

import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.bp.ActiveCache;
import com.ytfs.service.servlet.CacheAccessor;
import com.ytfs.service.servlet.Handler;

public class ActiveCacheHandler extends Handler<ActiveCache> {

    @Override
    public Object handle() throws Throwable {
        try {
            CacheAccessor.getUploadBlockCache(request.getVBI());
        } catch (Exception r) {
        }
        return new VoidResp();
    }

}
