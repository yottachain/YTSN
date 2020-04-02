package com.ytfs.service.servlet.v2;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.service.dao.BlockAccessor;
import com.ytfs.service.dao.BlockMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.servlet.Handler;
import com.ytfs.service.servlet.bp.SaveObjectMetaHandler;
import static com.ytfs.common.ServiceErrorCode.INVALID_KEU;
import static com.ytfs.common.ServiceErrorCode.NO_SUCH_BLOCK;
import com.ytfs.common.ServiceException;
import com.ytfs.common.codec.ShardEncoder;
import com.ytfs.service.packet.ObjectRefer;
import com.ytfs.service.packet.bp.SaveObjectMetaReq;
import com.ytfs.service.packet.bp.SaveObjectMetaResp;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.v2.UploadBlockDupReqV2;
import org.apache.log4j.Logger;

public class UploadBlockDupHandler extends Handler<UploadBlockDupReqV2> {

    private static final Logger LOG = Logger.getLogger(UploadBlockDupHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser(request);
        if (user == null) {
            return new ServiceException(ServiceErrorCode.INVALID_SIGNATURE);
        }
        int userid = user.getUserID();
        LOG.info("Upload block " + user.getUserID() + "/" + request.getVNU() + "/" + request.getId() + " exist...");
        BlockMeta meta = BlockAccessor.getBlockMeta(request.getVHP(), request.getVHB());
        if (meta == null) {
            throw new ServiceException(NO_SUCH_BLOCK);
        }
        verify();
        SaveObjectMetaReq saveObjectMetaReq = makeSaveObjectMetaReq(userid, meta.getVBI(), request.getKeyNumber());
        long usedSpace = meta.getAR() == ShardEncoder.AR_DB_MODE ? ServerConfig.PCM : ServerConfig.PFL * meta.getVNF() * ServerConfig.space_factor / 100;
        saveObjectMetaReq.setUsedSpace(usedSpace);
        try {
            SaveObjectMetaResp resp = SaveObjectMetaHandler.saveObjectMetaCall(saveObjectMetaReq);
            if (resp.isExists()) {
                LOG.warn("Block " + user.getUserID() + "/" + request.getVNU() + "/" + request.getId() + " has been uploaded.");
            } else {
                BlockAccessor.incBlockNLINK(meta, 1);
            }
        } catch (ServiceException r) {
            throw r;
        }
        return new VoidResp();
    }

    private void verify() throws ServiceException {
        if (request.getKEU() == null || request.getKEU().length != 32) {
            throw new ServiceException(INVALID_KEU);
        }
    }

    private SaveObjectMetaReq makeSaveObjectMetaReq(int userid, long vbi, int keyNumber) {
        SaveObjectMetaReq saveObjectMetaReq = new SaveObjectMetaReq();
        saveObjectMetaReq.setUserID(userid);
        saveObjectMetaReq.setVNU(request.getVNU());
        ObjectRefer refer = new ObjectRefer();
        refer.setVBI(vbi);
        refer.setId(request.getId());
        refer.setKEU(request.getKEU());
        refer.setOriginalSize(request.getOriginalSize());
        refer.setRealSize(request.getRealSize());
        refer.setSuperID((byte) ServerConfig.superNodeID);
        refer.setKeyNumber(keyNumber);
        saveObjectMetaReq.setRefer(refer);
        return saveObjectMetaReq;
    }
}
