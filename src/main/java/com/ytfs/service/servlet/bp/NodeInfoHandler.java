package com.ytfs.service.servlet.bp;

import com.ytfs.service.packet.bp.NodeInfoReq;
import com.ytfs.service.packet.bp.NodeInfoResp;
import com.ytfs.service.servlet.Handler;

public class NodeInfoHandler extends Handler<NodeInfoReq> {

    @Override
    public NodeInfoResp handle() throws Throwable {
        NodeInfoResp resp=new NodeInfoResp();
        //resp.setNode(node);
        return resp;
    }

}
