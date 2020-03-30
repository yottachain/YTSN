package com.ytfs.service.servlet.v2;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.eos.EOSClient;
import com.ytfs.service.dao.CacheBaseAccessor;
import com.ytfs.service.dao.ObjectAccessor;
import com.ytfs.service.dao.ObjectMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserAccessor;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.v2.UploadObjectEndReqV2;
import com.ytfs.service.servlet.CacheAccessor;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

public class UploadObjectEndHandler extends Handler<UploadObjectEndReqV2> {

    private static final Logger LOG = Logger.getLogger(UploadObjectEndHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser(request);
        if (user == null) {
            return new ServiceException(ServiceErrorCode.INVALID_SIGNATURE);
        }
        if (CacheAccessor.getUploadObjectCache(request.getVNU()) == null) {
            LOG.warn(request.getVNU() + " already completed.");
            return new ServiceException(ServiceErrorCode.INVALID_UPLOAD_ID);
        }
        int userid = user.getUserID();
        ObjectMeta meta = new ObjectMeta(userid, request.getVHW());
        ObjectAccessor.getObjectAndUpdateNLINK(meta);
        long usedspace = meta.getUsedspace();
        long ll = 1024L * 16L;
        long addusedspace = usedspace % ll > 0 ? (usedspace / ll + 1) : (usedspace / ll);
        UserAccessor.updateUser(userid, usedspace, 1, meta.getLength());
        try {
            EOSClient.addUsedSpace(addusedspace, user.getUsername());
            LOG.info("User " + user.getUserID() + " add usedSpace:" + usedspace);
        } catch (Throwable e) {
            CacheBaseAccessor.addNewObject(meta.getVNU(), addusedspace, user.getUserID(), user.getUsername(), 0);
            LOG.error("Add usedSpace ERR:" + e.getMessage());
            LOG.info("Upload object " + user.getUserID() + "/" + meta.getVNU() + " OK.");
            CacheAccessor.delUploadObjectCache(meta.getVNU());
            return new VoidResp();
        }
        long firstCost = ServerConfig.unitFirstCost * usedspace / ServerConfig.unitSpace;
        try {
            EOSClient.deductHDD(firstCost, user.getUsername());
            LOG.info("User " + user.getUserID() + " sub Balance:" + firstCost);
        } catch (Throwable e) {
            CacheBaseAccessor.addNewObject(meta.getVNU(), usedspace, user.getUserID(), user.getUsername(), 1);
            LOG.error("Sub Balance ERR:" + e.getMessage());
        }
        LOG.info("Upload object " + user.getUserID() + "/" + meta.getVNU() + " OK.");
        CacheAccessor.delUploadObjectCache(meta.getVNU());
        return new VoidResp();
    }

}
