package com.ytfs.service.servlet.user;

import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.bp.ActiveCache;
import com.ytfs.service.servlet.CacheAccessor;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

public class ActiveCacheHandler extends Handler<ActiveCache> {
    
    private static final Logger LOG = Logger.getLogger(ActiveCacheHandler.class);
    
    @Override
    public Object handle() throws Throwable {   
        try {
            CacheAccessor.getUploadBlockCache(request.getVBI());
        } catch (Exception r) {
        }
        return new VoidResp();
    }
    
}
