package com.ytfs.service.servlet;

import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserCache;
import com.ytfs.common.node.NodeCache;
import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import io.jafka.jeos.util.Base58;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import org.apache.log4j.Logger;

public abstract class Handler<T> {

    private static final Logger LOG = Logger.getLogger(Handler.class);

    protected T request;
    private String pubkey;

    public final void setRequest(T req) {
        this.request = req;
    }

    public final void setPubkey(String key) {
        this.pubkey = key;
    }

    public final String getPublicKey() {
        return pubkey;
    }

    protected final User getUser() throws ServiceException {
        User user = UserCache.getUser(pubkey);
        if (user == null) {
            LOG.warn("Invalid user public key:" + pubkey);
            throw new ServiceException(ServiceErrorCode.INVALID_USER_ID);
        }
        return user;
    }

    protected final int getSuperNodeId() throws NodeMgmtException {
        return NodeCache.getSuperNodeId(pubkey);
    }

    protected final int getNodeId() throws NodeMgmtException {
        return NodeCache.getNodeId(pubkey);
    }

    public abstract Object handle() throws Throwable;
}
