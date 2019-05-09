package com.ytfs.service.servlet;

import com.ytfs.service.ServerConfig;
import com.ytfs.service.UserConfig;
import com.ytfs.service.packet.ObjectRefer;
import com.ytfs.service.dao.ObjectAccessor;
import com.ytfs.service.dao.ObjectMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserAccessor;
import com.ytfs.service.eos.EOSClient;
import com.ytfs.service.eos.EOSRequest;
import com.ytfs.service.node.SuperNodeList;
import com.ytfs.service.packet.GetBalanceReq;
import com.ytfs.service.packet.SubBalanceReq;
import com.ytfs.service.utils.ServiceErrorCode;
import com.ytfs.service.utils.ServiceException;
import com.ytfs.service.packet.UploadObjectEndReq;
import com.ytfs.service.packet.UploadObjectEndResp;
import com.ytfs.service.packet.UploadObjectInitReq;
import com.ytfs.service.packet.UploadObjectInitResp;
import com.ytfs.service.packet.VoidResp;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.List;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

public class UploadObjectHandler {

    private static final Logger LOG = Logger.getLogger(UploadObjectHandler.class);

    /**
     * 初始化上传对象
     *
     * @param ud
     * @param userid
     * @return UploadObjectInitResp
     * @throws ServiceException
     * @throws Throwable
     */
    static UploadObjectInitResp init(UploadObjectInitReq ud, User user) throws ServiceException, Throwable {
        LOG.info("Upload object init " + user.getUserID());
        int userid = user.getUserID();
        SuperNode n = SuperNodeList.getUserSuperNode(userid);
        if (n.getId() != ServerConfig.superNodeID) {
            throw new ServiceException(ServiceErrorCode.INVALID_USER_ID);
        }
        if (ud.getVHW() == null || ud.getVHW().length != 32) {
            throw new ServiceException(ServiceErrorCode.INVALID_VHW);
        }
        ObjectMeta meta = new ObjectMeta(userid, ud.getVHW());
        boolean exists = ObjectAccessor.isObjectExists(meta);
        UploadObjectInitResp resp = new UploadObjectInitResp(false);
        if (exists) {
            resp.setVNU(meta.getVNU());
            int nlink = meta.getNLINK();
            if (nlink == 0) {//正在上传               
                List<ObjectRefer> refers = ObjectRefer.parse(meta.getBlocks());
                short[] blocks = new short[refers.size()];
                for (int ii = 0; ii < blocks.length; ii++) {
                    blocks[ii] = refers.get(ii).getId();
                }
                resp.setBlocks(blocks);
            } else {
                ObjectAccessor.incObjectNLINK(meta);
                resp.setRepeat(true);
                return resp;
            }
        } else {
            meta.setVNU(new ObjectId());
            resp.setVNU(meta.getVNU());
        }
        byte[] signarg = EOSRequest.createEosClient(meta.getVNU());
        resp.setSignArg(signarg);
        return resp;
    }

    /**
     * 获取余额
     *
     * @param getBalanceReq
     * @param user
     * @return VoidResp
     * @throws ServiceException 余额不足
     * @throws Throwable
     */
    static VoidResp getBalanceReq(GetBalanceReq getBalanceReq, User user) throws ServiceException, Throwable {
        LOG.info("Get Balance:" + user.getUserID());
        boolean has = EOSClient.hasSpace(getBalanceReq.getLength(), getBalanceReq.getSignData(), getBalanceReq.getVNU());
        if (has) {
            ObjectMeta meta = new ObjectMeta(user.getUserID(), getBalanceReq.getVHW());
            meta.setLength(getBalanceReq.getLength());
            meta.setVNU(getBalanceReq.getVNU());
            meta.setNLINK(0);
            ObjectAccessor.insertOrUpdate(meta);
        } else {
            throw new ServiceException(ServiceErrorCode.NOT_ENOUGH_DHH);
        }
        return new VoidResp();
    }

    /**
     * 上传对象完毕
     *
     * @param req
     * @param userid
     * @return UploadObjectEndResp
     * @throws Throwable
     */
    static UploadObjectEndResp complete(UploadObjectEndReq req, User user) throws Throwable {
        int userid = user.getUserID();
        ObjectMeta meta = new ObjectMeta(userid, req.getVHW());
        ObjectAccessor.getObjectAndUpdateNLINK(meta);
        long usedspace = meta.getUsedspace() + ServerConfig.PCM;
        long count = usedspace / UserConfig.Default_Shard_Size
                + (usedspace % UserConfig.Default_Shard_Size > 0 ? 1 : 0);
        long costPerCycle = count * ServerConfig.unitcost;
        ObjectAccessor.addNewObject(meta.getVNU(), costPerCycle, user.getUserID());
        UserAccessor.updateUser(userid, usedspace, 1, meta.getLength());
        long firstCost = costPerCycle * ServerConfig.PMS;
        byte[] signarg = EOSRequest.createEosClient(meta.getVNU());
        UploadObjectEndResp resp = new UploadObjectEndResp();
        resp.setFirstCost(firstCost);
        resp.setSignArg(signarg);
        LOG.info("Upload object " + user.getUserID() + "/" + meta.getVNU() + " OK.");
        return resp;
    }

    /**
     * 扣除初始HDD
     *
     * @param req
     * @param user
     * @return VoidResp
     * @throws Throwable
     */
    static VoidResp subBalanceReq(SubBalanceReq req, User user) throws Throwable {
        LOG.info("Sub Balance:" + user.getUserID());
        EOSClient.deductHDD(req.getSignData(), req.getVNU());
        return new VoidResp();
    }

}
