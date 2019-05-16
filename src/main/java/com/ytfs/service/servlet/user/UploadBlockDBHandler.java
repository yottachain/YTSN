package com.ytfs.service.servlet.user;

import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.codec.BlockEncrypted;
import com.ytfs.service.dao.BlockAccessor;
import com.ytfs.service.dao.BlockMeta;
import com.ytfs.service.dao.Sequence;
import com.ytfs.service.dao.User;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.servlet.CacheAccessor;
import com.ytfs.service.servlet.Handler;
import com.ytfs.service.servlet.UploadObjectCache;
import com.ytfs.service.servlet.bp.SaveObjectMetaHandler;
import static com.ytfs.common.ServiceErrorCode.ILLEGAL_VHP_NODEID;
import static com.ytfs.common.ServiceErrorCode.INVALID_KED;
import static com.ytfs.common.ServiceErrorCode.INVALID_KEU;
import static com.ytfs.common.ServiceErrorCode.INVALID_VHB;
import static com.ytfs.common.ServiceErrorCode.INVALID_VHP;
import static com.ytfs.common.ServiceErrorCode.TOO_BIG_BLOCK;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.ObjectRefer;
import com.ytfs.service.packet.SaveObjectMetaReq;
import com.ytfs.service.packet.SaveObjectMetaResp;
import com.ytfs.service.packet.UploadBlockDBReq;
import com.ytfs.service.packet.VoidResp;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.Arrays;
import org.apache.log4j.Logger;

public class UploadBlockDBHandler extends Handler<UploadBlockDBReq> {

    private static final Logger LOG = Logger.getLogger(UploadBlockDBHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        int userid = user.getUserID();
        LOG.info("Upload block " + user.getUserID() + "/" + request.getVNU() + "/" + request.getId() + " to DB...");
        UploadObjectCache progress = CacheAccessor.getUploadObjectCache(userid, request.getVNU());
        if (progress.exists(request.getId())) {
            return new VoidResp();
        }
        SuperNode n = SuperNodeList.getBlockSuperNode(request.getVHP());
        if (n.getId() != ServerConfig.superNodeID) {//验证数据块是否对应
            throw new ServiceException(ILLEGAL_VHP_NODEID);
        }
        BlockEncrypted b = new BlockEncrypted();
        b.setData(request.getData());
        verify(request, b.getVHB());
        BlockMeta meta = makeBlockMeta(request, Sequence.generateBlockID(1));
        BlockAccessor.saveBlockData(meta.getVBI(), request.getData());
        BlockAccessor.saveBlockMeta(meta);
        SaveObjectMetaReq saveObjectMetaReq = makeSaveObjectMetaReq(request, userid, meta.getVBI());
        saveObjectMetaReq.setNlink(1);
        try {
            SaveObjectMetaResp resp = SaveObjectMetaHandler.saveObjectMetaCall(saveObjectMetaReq);
            progress.setBlockNum(request.getId());
            if (resp.isExists()) {
                BlockAccessor.decBlockNLINK(meta);//-1
            }
        } catch (ServiceException r) {
            BlockAccessor.decBlockNLINK(meta);//-1
            throw r;
        }
        return new VoidResp();
    }
 
    private void verify(UploadBlockDBReq req, byte[] vhb) throws ServiceException {
        if (!Arrays.equals(vhb, req.getVHB())) {
            throw new ServiceException(INVALID_VHB);
        }
        if (req.getVHP() == null || req.getVHP().length != 32) {
            throw new ServiceException(INVALID_VHP);
        }
        if (req.getKEU() == null || req.getKEU().length != 32) {
            throw new ServiceException(INVALID_KEU);
        }
        if (req.getKED() == null || req.getKED().length != 32) {
            throw new ServiceException(INVALID_KED);
        }
        if (req.getData().length > ServerConfig.PL2 * 2) {
            throw new ServiceException(TOO_BIG_BLOCK);
        }
    }

    private BlockMeta makeBlockMeta(UploadBlockDBReq req, long VBI) {
        BlockMeta meta = new BlockMeta();
        meta.setVBI(VBI);
        meta.setKED(req.getKED());
        meta.setNLINK(1);
        meta.setVNF(0);
        meta.setVHB(req.getVHB());
        meta.setVHP(req.getVHP());
        return meta;
    }

    private SaveObjectMetaReq makeSaveObjectMetaReq(UploadBlockDBReq req, int userid, long vbi) {
        SaveObjectMetaReq saveObjectMetaReq = new SaveObjectMetaReq();
        saveObjectMetaReq.setUserID(userid);
        saveObjectMetaReq.setVNU(req.getVNU());
        ObjectRefer refer = new ObjectRefer();
        refer.setId(req.getId());
        refer.setKEU(req.getKEU());
        refer.setOriginalSize(req.getOriginalSize());
        refer.setRealSize(req.getData().length);
        refer.setSuperID((byte) ServerConfig.superNodeID);
        refer.setVBI(vbi);
        saveObjectMetaReq.setRefer(refer);
        return saveObjectMetaReq;
    }
}
