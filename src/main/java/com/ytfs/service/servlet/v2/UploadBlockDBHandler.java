package com.ytfs.service.servlet.v2;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.codec.BlockEncrypted;
import com.ytfs.service.dao.BlockAccessor;
import com.ytfs.service.dao.BlockMeta;
import com.ytfs.service.dao.Sequence;
import com.ytfs.service.dao.User;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.servlet.Handler;
import com.ytfs.service.servlet.bp.SaveObjectMetaHandler;
import static com.ytfs.common.ServiceErrorCode.ILLEGAL_VHP_NODEID;
import static com.ytfs.common.ServiceErrorCode.INVALID_KED;
import static com.ytfs.common.ServiceErrorCode.INVALID_KEU;
import static com.ytfs.common.ServiceErrorCode.INVALID_VHB;
import static com.ytfs.common.ServiceErrorCode.INVALID_VHP;
import static com.ytfs.common.ServiceErrorCode.TOO_BIG_BLOCK;
import com.ytfs.common.ServiceException;
import com.ytfs.common.codec.ShardEncoder;
import com.ytfs.service.packet.ObjectRefer;
import com.ytfs.service.packet.bp.SaveObjectMetaReq;
import com.ytfs.service.packet.bp.SaveObjectMetaResp;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.v2.UploadBlockDBReqV2;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.Arrays;
import org.apache.log4j.Logger;

public class UploadBlockDBHandler extends Handler<UploadBlockDBReqV2> {

    private static final Logger LOG = Logger.getLogger(UploadBlockDBHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser(request);
        if (user == null) {
            return new ServiceException(ServiceErrorCode.INVALID_SIGNATURE);
        }
        int userid = user.getUserID();
        LOG.info("Save block " + user.getUserID() + "/" + request.getVNU() + "/" + request.getId() + " to DB...");
        SuperNode n = SuperNodeList.getBlockSuperNode(request.getVHP());
        if (n.getId() != ServerConfig.superNodeID) {//验证数据块是否对应
            throw new ServiceException(ILLEGAL_VHP_NODEID);
        }
        long VBI = Sequence.generateBlockID(1);
        BlockMeta bmeta = BlockAccessor.getBlockMeta(request.getVHP(), request.getVHB());
        if (bmeta != null) {
            if (!Arrays.equals(bmeta.getKED(), request.getKED())) {
                LOG.error("Block meta duplicate writing.");
                return new ServiceException(ServiceErrorCode.SERVER_ERROR);
            } else {
                VBI = bmeta.getVBI();
            }
        }
        BlockEncrypted b = new BlockEncrypted();
        b.setData(request.getData());
        verify(b.getVHB());
        BlockAccessor.saveBlockData(VBI, request.getData());
        if (bmeta == null) {
            BlockMeta meta = makeBlockMeta(VBI);
            BlockAccessor.saveBlockMeta(meta);
        }
        SaveObjectMetaReq saveObjectMetaReq = makeSaveObjectMetaReq(userid, VBI, request.getKeyNumber());
        saveObjectMetaReq.setUsedSpace(ServerConfig.PCM);
        try {
            SaveObjectMetaResp resp = SaveObjectMetaHandler.saveObjectMetaCall(saveObjectMetaReq);
            if (resp.isExists()) {
                LOG.warn("Block " + user.getUserID() + "/" + request.getVNU() + "/" + request.getId() + " has been uploaded.");
            }
        } catch (ServiceException r) {
            throw r;
        }
        return new VoidResp();
    }

    private void verify(byte[] vhb) throws ServiceException {
        if (!Arrays.equals(vhb, request.getVHB())) {
            throw new ServiceException(INVALID_VHB);
        }
        if (request.getVHP() == null || request.getVHP().length != 32) {
            throw new ServiceException(INVALID_VHP);
        }
        if (request.getKEU() == null || request.getKEU().length != 32) {
            throw new ServiceException(INVALID_KEU);
        }
        if (request.getKED() == null || request.getKED().length != 32) {
            throw new ServiceException(INVALID_KED);
        }
        if (request.getData().length > ServerConfig.PL2 * 2) {
            throw new ServiceException(TOO_BIG_BLOCK);
        }
    }

    private BlockMeta makeBlockMeta(long VBI) {
        BlockMeta meta = new BlockMeta();
        meta.setVBI(VBI);
        meta.setKED(request.getKED());
        meta.setNLINK(1);
        meta.setVNF(0);
        meta.setAR(ShardEncoder.AR_DB_MODE);
        meta.setVHB(request.getVHB());
        meta.setVHP(request.getVHP());
        return meta;
    }

    private SaveObjectMetaReq makeSaveObjectMetaReq(int userid, long vbi, int keyNumber) {
        SaveObjectMetaReq saveObjectMetaReq = new SaveObjectMetaReq();
        saveObjectMetaReq.setUserID(userid);
        saveObjectMetaReq.setVNU(request.getVNU());
        ObjectRefer refer = new ObjectRefer();
        refer.setId(request.getId());
        refer.setKEU(request.getKEU());
        refer.setOriginalSize(request.getOriginalSize());
        refer.setRealSize(request.getData().length);
        refer.setSuperID((byte) ServerConfig.superNodeID);
        refer.setVBI(vbi);
        refer.setKeyNumber(keyNumber);
        saveObjectMetaReq.setRefer(refer);
        return saveObjectMetaReq;
    }
}
