package com.ytfs.service.servlet.user;

import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserAccessor;
import com.ytfs.service.packet.RegUserReq;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import io.yottachain.p2phost.utils.Base58;

public class RegUserHandler extends Handler<RegUserReq> {

    @Override
    public Object handle() throws Throwable {
        byte[] KUEp = Base58.decode(request.getPubkey());
        User user = UserAccessor.getUser(KUEp);
        if (user != null) {
            SuperNode sn = SuperNodeList.getUserSuperNode(user.getUserID());
            
        }
        return null;
    }

}
