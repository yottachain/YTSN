package com.ytfs.service.servlet.user;

import com.ytfs.common.node.NodeManager;
import com.ytfs.service.packet.user.ListSuperNodeReq;
import com.ytfs.service.packet.user.ListSuperNodeResp;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import org.apache.log4j.Logger;

public class ListSuperNodeHandler extends Handler<ListSuperNodeReq> {

    private static final Logger LOG = Logger.getLogger(ListSuperNodeHandler.class);

    @Override
    public Object handle() throws Throwable {
        ListSuperNodeResp resp = new ListSuperNodeResp();
        LOG.info("List super node...");
        SuperNode[] sn = NodeManager.getSuperNode();
        resp.setSuperList(sn);
        return resp;
    }

}
