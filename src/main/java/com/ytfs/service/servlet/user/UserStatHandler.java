package com.ytfs.service.servlet.user;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.User;
import com.ytfs.service.http.LocalHttpHandler;
import com.ytfs.service.packet.user.UserSpaceReq;
import com.ytfs.service.packet.user.UserSpaceResp;
import com.ytfs.service.servlet.Handler;

public class UserStatHandler extends Handler<UserSpaceReq> {

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        if(user==null){
            return new ServiceException(ServiceErrorCode.NEED_LOGIN);
        }
        String json = LocalHttpHandler.getusertotal(Integer.toString(user.getUserID()));
        UserSpaceResp resp = new UserSpaceResp();
        resp.setJson(json);
        return resp;
    }

}
