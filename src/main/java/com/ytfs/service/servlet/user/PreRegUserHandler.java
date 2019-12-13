package com.ytfs.service.servlet.user;

import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.eos.EOSRequest;
import com.ytfs.service.packet.user.PreRegUserReq;
import com.ytfs.service.packet.user.PreRegUserResp;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

public class PreRegUserHandler extends Handler<PreRegUserReq> {

    private static final Logger LOG = Logger.getLogger(PreRegUserHandler.class);

    @Override
    public Object handle() throws Throwable {
        LOG.debug("PreRegUserReq Received.");
        byte[] signarg = EOSRequest.createEosClient(this.getPublicKey());
        PreRegUserResp resp = new PreRegUserResp();
        resp.setSignArg(signarg);
        resp.setContractAccount(ServerConfig.contractAccount);
        return resp;
    }
}
