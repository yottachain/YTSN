package com.ytfs.service.servlet.user;

import com.ytfs.service.dao.User;
import com.ytfs.service.http.LocalHttpHandler;
import com.ytfs.service.packet.user.UserSpaceReq;
import com.ytfs.service.packet.user.UserSpaceResp;
import com.ytfs.service.servlet.Handler;

public class UserStatHandler extends Handler<UserSpaceReq> {

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        String json = LocalHttpHandler.getusertotal(Integer.toString(user.getUserID()));
        UserSpaceResp resp = new UserSpaceResp();
        resp.setJson(json);
        return resp;
    }

}
