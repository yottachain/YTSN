package com.ytfs.service.servlet;

import com.ytfs.service.ServerConfig;
import com.ytfs.service.packet.ObjectRefer;
import com.ytfs.service.dao.ObjectAccessor;
import com.ytfs.service.dao.ObjectMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserAccessor;
import com.ytfs.service.eos.EOSClient;
import com.ytfs.service.eos.EOSRequest;
import com.ytfs.service.node.SuperNodeList;
import com.ytfs.service.packet.GetBalanceReq;
import com.ytfs.service.packet.ServiceErrorCode;
import com.ytfs.service.packet.ServiceException;
import com.ytfs.service.packet.UploadObjectEndReq;
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
     * 上传对象完毕
     *
     * @param req
     * @param userid
     * @return
     * @throws ServiceException
     * @throws Throwable
     */
    static VoidResp complete(UploadObjectEndReq req, User user) throws ServiceException, Throwable {
        int userid = user.getUserID();
        ObjectMeta meta = new ObjectMeta(userid, req.getVHW());
        ObjectAccessor.getObjectAndUpdateNLINK(meta);
        long usedspace = meta.getUsedspace();
        
        ObjectAccessor.addNewObject(meta.getVNU());
        UserAccessor.updateUser(userid, usedspace, 1, meta.getLength());

        List<ObjectRefer> refers = ObjectRefer.parse(meta.getBlocks());
        long size = 0;
        for (ObjectRefer refer : refers) {
            size = size + refer.getRealSize();
        }
        // size = ServerConfig.PMS + size;
        //  EOSClient eos = new EOSClient(user.getEosName());
        // eos.freeHDD(meta.getLength());
        // eos.deductHDD(size);
        LOG.info("Upload object " + user.getUserID() + "/" + meta.getVNU() + " OK.");
        return new VoidResp();
    }

    /**
     * 初始化上传对象
     *
     * @param ud
     * @param userid
     * @return
     * @throws ServiceException
     * @throws Throwable
     */
    static UploadObjectInitResp init(UploadObjectInitReq ud, User user) throws ServiceException, Throwable {
        LOG.info("Upload object init " + user.getUserID());
        int userid = user.getUserID();
        SuperNode n = SuperNodeList.getBlockSuperNodeByUserId(userid);
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
        }
        byte[] signarg = EOSRequest.createEosClient(meta.getVNU());
        resp.setSignArg(signarg);
        return resp;
    }

    static Object getBalanceReq(GetBalanceReq getBalanceReq, User user) throws ServiceException, Throwable {
        LOG.info("Get Balance:" + user.getUserID());
        boolean has = EOSClient.hasSpace(getBalanceReq.getLength(), getBalanceReq.getSignData(), getBalanceReq.getVNU());
        if (has) {
            ObjectMeta meta = new ObjectMeta(user.getUserID(), getBalanceReq.getVHW());
            meta.setLength(getBalanceReq.getLength());
            meta.setVNU(getBalanceReq.getVNU());
            meta.setNLINK(0);
            ObjectAccessor.insertOrUpdate(meta);
        }
        return new VoidResp();
    }
}
