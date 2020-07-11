package com.ytfs.service.servlet.user;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import static com.ytfs.common.ServiceErrorCode.TOO_LOW_VERSION;
import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.ServerConfig;
import static com.ytfs.common.conf.ServerConfig.s3Version;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.SNSynchronizer;
import com.ytfs.service.packet.user.QueryUserReq;
import com.ytfs.service.packet.user.QueryUserResp;
import com.ytfs.service.packet.user.RegUserReq;
import com.ytfs.service.packet.user.RegUserResp;
import com.ytfs.service.servlet.Handler;
import static com.ytfs.service.servlet.user.QueryUserHandler.queryAndReg;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

public class RegUserHandler extends Handler<RegUserReq> {

    private static final Logger LOG = Logger.getLogger(RegUserHandler.class);
    public static Cache<String, Long> REG_CACHE = CacheBuilder.newBuilder()
            .expireAfterWrite(5, TimeUnit.SECONDS)
            .maximumSize(10000)
            .build();

    ;

    @Override
    public Object handle() throws Throwable {
        String cachekey = this.getPublicKey();
        if (REG_CACHE.getIfPresent(cachekey) != null) {
            LOG.error("UserLogin:" + request.getUsername() + " ERR:too frequently");
            return new ServiceException(SERVER_ERROR);
        } else {
            REG_CACHE.put(cachekey, System.currentTimeMillis());
        }
        LOG.info("UserLogin:" + request.getUsername());
        if (s3Version != null) {
            if (request.getVersionId() == null || request.getVersionId().compareTo(s3Version) < 0) {
                LOG.error("UserLogin:" + request.getUsername() + " ERR:TOO_LOW_VERSION?" + request.getVersionId());
                return new ServiceException(TOO_LOW_VERSION);
            }
        }
        String userPubkey = request.getPubKey();
        SuperNode sn = SuperNodeList.getUserRegSuperNode(request.getUsername());
        QueryUserReq req = new QueryUserReq();
        req.setCacheKey(cachekey);
        req.setUsername(request.getUsername());
        req.setPubkey(userPubkey);
        Object obj;
        if (sn.getId() == ServerConfig.superNodeID) {
            obj = queryAndReg(req);
        } else {
            obj = P2PUtils.requestBP(req, sn);
        }
        if (!(obj instanceof QueryUserResp)) {
            return obj instanceof ServiceException ? (ServiceException) obj : new ServiceException(SERVER_ERROR);
        }
        QueryUserResp resp = (QueryUserResp) obj;
        req.setUserId(resp.getUserId());
        LOG.info("[" + request.getUsername() + "] is registered @ SN-" + sn.getId() + ",userID:" + resp.getUserId());
        Object[] obs = SNSynchronizer.syncRequest(req, sn.getId());
        for (Object o : obs) {
            if (o != null) {
                if (o instanceof ServiceException) {
                    LOG.error("Sync userinfo ERR:" + ((ServiceException) o).getErrorCode());
                    return new ServiceException(SERVER_ERROR);
                }
            }
        }
        RegUserResp regUserResp = new RegUserResp();
        SuperNode ressn = SuperNodeList.getUserSuperNode(resp.getUserId());
        if (ressn.getId() != sn.getId()) {
            LOG.error("SuperID inconsistency[" + ressn.getId() + "!=" + sn.getId() + "]");
            return new ServiceException(SERVER_ERROR);
        }
        regUserResp.setSuperNodeAddrs(ressn.getAddrs());
        regUserResp.setSuperNodeID(ressn.getNodeid());
        regUserResp.setSuperNodeNum(ressn.getId());
        regUserResp.setUserId(resp.getUserId());
        regUserResp.setKeyNumber(resp.getKeyNumber());
        return regUserResp;
    }

}
