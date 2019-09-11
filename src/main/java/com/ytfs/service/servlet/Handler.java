package com.ytfs.service.servlet;

import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserCache;
import com.ytfs.common.node.NodeCache;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;

public abstract class Handler<T> {

 
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

    protected final User getUser() {
        return UserCache.getUser(pubkey);
    }

    protected final int getSuperNodeId() throws NodeMgmtException {
        return NodeCache.getSuperNodeId(pubkey);
    }

    protected final int getNodeId() throws NodeMgmtException {
        return NodeCache.getNodeId(pubkey);
    }

    public abstract Object handle() throws Throwable;
}
