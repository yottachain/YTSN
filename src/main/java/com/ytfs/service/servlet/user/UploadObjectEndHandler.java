package com.ytfs.service.servlet.user;

import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.eos.EOSClient;
import com.ytfs.service.dao.ObjectAccessor;
import com.ytfs.service.dao.ObjectMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserAccessor;
import com.ytfs.common.eos.EOSRequest;
import com.ytfs.service.packet.UploadObjectEndReq;
import com.ytfs.service.packet.UploadObjectEndResp;
import com.ytfs.service.servlet.CacheAccessor;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

public class UploadObjectEndHandler extends Handler<UploadObjectEndReq> {

    private static final Logger LOG = Logger.getLogger(UploadObjectEndHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        int userid = user.getUserID();
        ObjectMeta meta = new ObjectMeta(userid, request.getVHW());
        ObjectAccessor.getObjectAndUpdateNLINK(meta);
        long usedspace = meta.getUsedspace() + ServerConfig.PCM;
        EOSClient.addUsedSpace(usedspace, user.getUsername(), userid);
        long count = usedspace / UserConfig.Default_Shard_Size
                + (usedspace % UserConfig.Default_Shard_Size > 0 ? 1 : 0);
        long costPerCycle = count * ServerConfig.unitcost;
        ObjectAccessor.addNewObject(meta.getVNU(), costPerCycle, user.getUserID(), user.getUsername());
        UserAccessor.updateUser(userid, usedspace, 1, meta.getLength());
        long firstCost = costPerCycle * ServerConfig.PMS;
        byte[] signarg = EOSRequest.createEosClient(meta.getVNU());
        UploadObjectEndResp resp = new UploadObjectEndResp();
        resp.setFirstCost(firstCost);
        resp.setSignArg(signarg);
        resp.setUserid(userid);
        resp.setContractAccount(ServerConfig.contractAccount);
        LOG.info("Upload object " + user.getUserID() + "/" + meta.getVNU() + " OK.");
        CacheAccessor.delUploadObjectCache(meta.getVNU());
        return resp;
    }
}
