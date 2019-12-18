package com.ytfs.service.servlet.bp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserAccessor;
import com.ytfs.service.packet.bp.UserSpaceReq;
import com.ytfs.service.packet.bp.UserSpaceResp;
import com.ytfs.service.servlet.Handler;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import org.apache.log4j.Logger;

public class UserStatHandler extends Handler<UserSpaceReq> {

    private static final Logger LOG = Logger.getLogger(UserStatHandler.class);

    @Override
    public Object handle() throws Throwable {
        try {
            getSuperNodeId();
        } catch (NodeMgmtException e) {
            LOG.error("Invalid super node pubkey:" + this.getPublicKey());
            return new ServiceException(ServiceErrorCode.INVALID_NODE_ID, e.getMessage());
        }
        String json = query(request.getUserid());
        UserSpaceResp resp = new UserSpaceResp();
        resp.setJson(json);
        return resp;
    }

    public static String query(int id) throws JsonProcessingException {
        User user = UserAccessor.getUser(id);
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("userID", user.getUserID());
        node.put("fileTotal", user.getFileTotal());
        node.put("spaceTotal", user.getSpaceTotal());
        node.put("usedspace", user.getUsedspace());
        return mapper.writeValueAsString(node);
    }

}
