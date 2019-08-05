package com.ytfs.service.servlet.user;

import com.ytfs.common.conf.ServerConfig;
import com.ytfs.service.dao.BlockAccessor;
import com.ytfs.service.dao.BlockMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.servlet.CacheAccessor;
import com.ytfs.service.servlet.Handler;
import com.ytfs.service.servlet.UploadObjectCache;
import com.ytfs.service.servlet.bp.SaveObjectMetaHandler;
import static com.ytfs.common.ServiceErrorCode.INVALID_KEU;
import static com.ytfs.common.ServiceErrorCode.NO_SUCH_BLOCK;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.ObjectRefer;
import com.ytfs.service.packet.bp.SaveObjectMetaReq;
import com.ytfs.service.packet.bp.SaveObjectMetaResp;
import com.ytfs.service.packet.UploadBlockDupReq;
import com.ytfs.service.packet.VoidResp;
import org.apache.log4j.Logger;

public class UploadBlockDupHandler extends Handler<UploadBlockDupReq> {

    private static final Logger LOG = Logger.getLogger(UploadBlockDupHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        int userid = user.getUserID();
        LOG.info("Upload block " + user.getUserID() + "/" + request.getVNU() + "/" + request.getId() + " exist...");
        UploadObjectCache progress = CacheAccessor.getUploadObjectCache(userid, request.getVNU());
        if (progress.exists(request.getId())) {
            return new VoidResp();
        }
        BlockMeta meta = BlockAccessor.getBlockMeta(request.getVHP(), request.getVHB());
        if (meta == null) {
            throw new ServiceException(NO_SUCH_BLOCK);
        }
        verify(request);
        BlockAccessor.incBlockNLINK(meta);//+1        
        SaveObjectMetaReq saveObjectMetaReq = makeSaveObjectMetaReq(request, userid, meta.getVBI());
        saveObjectMetaReq.setNlink(meta.getNLINK() + 1);
        try {
            SaveObjectMetaResp resp = SaveObjectMetaHandler.saveObjectMetaCall(saveObjectMetaReq);
            progress.setBlockNum(request.getId());
            if (resp.isExists()) {
                LOG.warn("Block " + user.getUserID() + "/" + request.getVNU() + "/" + request.getId() + " has been uploaded.");
            }
        } catch (ServiceException r) {
            throw r;
        }
        return new VoidResp();
    }

    private void verify(UploadBlockDupReq req) throws ServiceException {
        if (req.getKEU() == null || req.getKEU().length != 32) {
            throw new ServiceException(INVALID_KEU);
        }
    }

    private SaveObjectMetaReq makeSaveObjectMetaReq(UploadBlockDupReq req, int userid, long vbi) {
        SaveObjectMetaReq saveObjectMetaReq = new SaveObjectMetaReq();
        saveObjectMetaReq.setUserID(userid);
        saveObjectMetaReq.setVNU(req.getVNU());
        ObjectRefer refer = new ObjectRefer();
        refer.setVBI(vbi);
        refer.setId(req.getId());
        refer.setKEU(req.getKEU());
        refer.setOriginalSize(req.getOriginalSize());
        refer.setRealSize(req.getRealSize());
        refer.setSuperID((byte) ServerConfig.superNodeID);
        saveObjectMetaReq.setRefer(refer);
        return saveObjectMetaReq;
    }
}
