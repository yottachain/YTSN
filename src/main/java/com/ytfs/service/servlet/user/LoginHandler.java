package com.ytfs.service.servlet.user;

import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserCache;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.user.LoginReq;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

public class LoginHandler extends Handler<LoginReq> {

    private static final Logger LOG = Logger.getLogger(LoginHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = UserCache.getUser(this.getPublicKey(), this.request.getUserId(), this.request.getSignData());
        LOG.info("User [" + user.getUsername() + "] login,id:" + user.getUserID());
        return new VoidResp();
    }

}