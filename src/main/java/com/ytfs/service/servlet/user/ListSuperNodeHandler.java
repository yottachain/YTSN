package com.ytfs.service.servlet.user;

import com.ytfs.service.node.NodeManager;
import com.ytfs.service.packet.ListSuperNodeReq;
import com.ytfs.service.packet.ListSuperNodeResp;
import com.ytfs.service.servlet.Handler;
import org.apache.log4j.Logger;

public class ListSuperNodeHandler extends Handler<ListSuperNodeReq> {

    private static final Logger LOG = Logger.getLogger(ListSuperNodeHandler.class);

    @Override
    public Object handle() throws Throwable {
        ListSuperNodeResp resp = new ListSuperNodeResp();
        LOG.info("List super node...");
        resp.setSuperList(NodeManager.getSuperNode());
        return resp;
    }

}
