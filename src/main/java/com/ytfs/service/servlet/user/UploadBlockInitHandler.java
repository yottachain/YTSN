package com.ytfs.service.servlet.user;

import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.service.dao.BlockAccessor;
import com.ytfs.service.dao.BlockMeta;
import com.ytfs.service.dao.User;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.servlet.Handler;
import static com.ytfs.common.ServiceErrorCode.ILLEGAL_VHP_NODEID;
import com.ytfs.common.ServiceException;
import static com.ytfs.service.ServiceWrapper.DE_DUPLICATION;
import com.ytfs.service.packet.user.UploadBlockDupResp;
import com.ytfs.service.packet.user.UploadBlockInitReq;
import com.ytfs.service.packet.user.UploadBlockInitResp;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.List;
import org.apache.log4j.Logger;

public class UploadBlockInitHandler extends Handler<UploadBlockInitReq> {

    private static final Logger LOG = Logger.getLogger(UploadBlockInitHandler.class);

    @Override
    public Object handle() throws Throwable {
        User user = this.getUser();
        if (user == null) {
            return new ServiceException(ServiceErrorCode.NEED_LOGIN);
        }
        LOG.info("Upload block init " + user.getUserID() + "/" + request.getVNU() + "/" + request.getId());
        SuperNode n = SuperNodeList.getBlockSuperNode(request.getVHP());
        if (n.getId() != ServerConfig.superNodeID) {//验证数据块是否对应
            throw new ServiceException(ILLEGAL_VHP_NODEID);
        }
        if (DE_DUPLICATION) {
            List<BlockMeta> ls = BlockAccessor.getBlockMeta(request.getVHP());
            if (ls.isEmpty()) {
                return new UploadBlockInitResp(System.currentTimeMillis());
            } else {
                UploadBlockDupResp resp = new UploadBlockDupResp();
                resp.setStartTime(System.currentTimeMillis());
                setKEDANDVHB(resp, ls);
                return resp;
            }
        } else {
            return new UploadBlockInitResp(System.currentTimeMillis());
        }
    }

    /**
     * 检查数据块是否重复,或强制请求分配node
     *
     * @param req
     * @param userid
     * @return 0已上传 1重复 2未上传
     * @throws ServiceException
     * @throws Throwable
     */
    private void setKEDANDVHB(UploadBlockDupResp resp, List<BlockMeta> ls) {
        byte[][] VHB = new byte[ls.size()][];
        byte[][] KED = new byte[ls.size()][];
        int[] ARS = new int[ls.size()];
        int index = 0;
        for (BlockMeta m : ls) {
            VHB[index] = m.getVHB();
            KED[index] = m.getKED();
            ARS[index] = m.getAR();
            index++;
        }
        resp.setVHB(VHB);
        resp.setKED(KED);
        resp.setAR(ARS);
    }

}
