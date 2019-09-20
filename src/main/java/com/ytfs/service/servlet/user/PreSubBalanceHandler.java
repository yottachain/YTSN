package com.ytfs.service.servlet.user;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.eos.EOSRequest;
import com.ytfs.service.dao.User;
import com.ytfs.service.packet.user.PreSubBalanceReq;
import com.ytfs.service.packet.user.PreSubBalanceResp;
import com.ytfs.service.servlet.CacheAccessor;
import com.ytfs.service.servlet.Handler;
import com.ytfs.service.servlet.UploadObjectCache;

public class PreSubBalanceHandler extends Handler<PreSubBalanceReq> {

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        if (user == null) {
            return new ServiceException(ServiceErrorCode.NEED_LOGIN);
        }
        UploadObjectCache cache = CacheAccessor.getUploadObjectCache(user.getUserID(), request.getVNU());
        long usedspace = cache.getUsedspace() + ServerConfig.PCM;
        long count = usedspace / UserConfig.Default_Shard_Size
                + (usedspace % UserConfig.Default_Shard_Size > 0 ? 1 : 0);
        long costPerCycle = count * ServerConfig.unitcost;
        long firstCost = costPerCycle * ServerConfig.PMS;
        byte[] signarg = EOSRequest.createEosClient(request.getVNU());
        PreSubBalanceResp resp = new PreSubBalanceResp();
        resp.setFirstCost(firstCost);
        resp.setSignArg(signarg);
        resp.setUserid(user.getUserID());
        resp.setContractAccount(ServerConfig.contractAccount);
        return resp;
    }

}
