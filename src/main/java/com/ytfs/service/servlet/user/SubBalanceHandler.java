package com.ytfs.service.servlet.user;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.User;
import com.ytfs.common.eos.EOSClient;
import com.ytfs.service.packet.SubBalanceReq;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

public class SubBalanceHandler extends Handler<SubBalanceReq> {

    private static final Logger LOG = Logger.getLogger(SubBalanceHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        if (user == null) {
            return new ServiceException(ServiceErrorCode.NEED_LOGIN);
        }
        try {
            EOSClient.deductHDD(request.getSignData(), request.getVNU());
            LOG.info("Sub Balance:" + user.getUserID());
        } catch (ServiceException e) {
            LOG.error("Sub Balance ERR: session expires.");
        }
        return new VoidResp();
    }

}
