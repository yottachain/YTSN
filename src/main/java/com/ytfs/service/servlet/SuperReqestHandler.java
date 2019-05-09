package com.ytfs.service.servlet;

import com.ytfs.service.ServerConfig;
import com.ytfs.service.packet.ObjectRefer;
import com.ytfs.service.dao.ObjectAccessor;
import com.ytfs.service.dao.ObjectMeta;
import com.ytfs.service.net.P2PUtils;
import com.ytfs.service.node.SuperNodeList;
import com.ytfs.service.packet.QueryObjectMetaReq;
import com.ytfs.service.packet.QueryObjectMetaResp;
import com.ytfs.service.packet.SaveObjectMetaReq;
import com.ytfs.service.packet.SaveObjectMetaResp;
import static com.ytfs.service.utils.ServiceErrorCode.INVALID_UPLOAD_ID;
import com.ytfs.service.utils.ServiceException;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.List;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

public class SuperReqestHandler {

    private static final Logger LOG = Logger.getLogger(SuperReqestHandler.class);

    /**
     * 保存上传进度至BPU
     *
     * @param req
     * @return
     * @throws ServiceException
     */
    static SaveObjectMetaResp saveObjectMetaCall(SaveObjectMetaReq req) throws ServiceException {
        SuperNode node = SuperNodeList.getUserSuperNode(req.getUserID());
        if (node.getId() == ServerConfig.superNodeID) {
            return saveObjectMeta(req);
        } else {
            return (SaveObjectMetaResp) P2PUtils.requestBP(req, node);
        }
    }

    private static long sumUsedSpace(long space, long nlink) {
        return space / nlink + 1;
    }

    /**
     * 存储数据块的超级节点需要向存储用户元数据的超级节点请求保存已上传完毕的块引用
     *
     * @param req
     * @return
     * @throws ServiceException
     */
    static SaveObjectMetaResp saveObjectMeta(SaveObjectMetaReq req) throws ServiceException {
        LOG.info("Save object meta:" + req.getUserID() + "/" + req.getVNU());
        SaveObjectMetaResp resp = new SaveObjectMetaResp();
        ObjectMeta meta = queryObjectMeta(req.getVNU(), req.getUserID());
        List<ObjectRefer> refers = ObjectRefer.parse(meta.getBlocks());
        resp.setExists(false);
        for (ObjectRefer refer : refers) {
            if (refer.getId() == req.getRefer().getId()) {
                resp.setExists(true);
                break;
            }
        }
        if (!resp.isExists()) {
            long usedspace = sumUsedSpace(req.getRefer().getRealSize(), req.getNlink());
            refers.add(req.getRefer());
            byte[] bs = ObjectRefer.merge(refers);
            ObjectAccessor.updateObject(req.getVNU(), bs, usedspace);
        }
        return resp;
    }

    /**
     * 存储数据块的超级节点需要向存储用户元数据的超级节点验证ＶＮＵ
     *
     * @param req
     * @return
     * @throws ServiceException
     */
    static QueryObjectMetaResp queryObjectMeta(QueryObjectMetaReq req) throws ServiceException {
        LOG.info("Query object meta:" + req.getUserID() + "/" + req.getVNU());
        ObjectMeta meta = queryObjectMeta(req.getVNU(), req.getUserID());
        QueryObjectMetaResp resp = new QueryObjectMetaResp(meta.getBlocks(), meta.getLength());
        return resp;
    }

    //查询数据库
    private static ObjectMeta queryObjectMeta(ObjectId VNU, int UserID) throws ServiceException {
        ObjectMeta meta = ObjectAccessor.getObject(VNU);
        if (meta == null) {
            throw new ServiceException(INVALID_UPLOAD_ID);
        } else {
            if (meta.getUserID() != UserID) {
                throw new ServiceException(INVALID_UPLOAD_ID);
            } else {
                return meta;
            }
        }
    }

}
