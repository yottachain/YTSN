package com.ytfs.service.servlet.user;

import com.ytfs.common.eos.EOSRequest;
import com.ytfs.service.packet.user.PreRegUserReq;
import com.ytfs.service.packet.user.PreRegUserResp;
import com.ytfs.service.servlet.Handler;

public class PreRegUserHandler extends Handler<PreRegUserReq> {

    @Override
    public Object handle() throws Throwable {
        byte[] signarg = EOSRequest.createEosClient(this.getPublicKey());
        PreRegUserResp resp = new PreRegUserResp();
        resp.setSignArg(signarg);
        return resp;
    }
}
