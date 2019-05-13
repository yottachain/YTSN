package com.ytfs.service.net;

import com.ytfs.service.SerializationUtil;
import com.ytfs.service.ServiceException;
import com.ytfs.service.packet.UploadShardReq;
import com.ytfs.service.packet.UploadShardResp;
import io.yottachain.p2phost.YottaP2P;
import io.yottachain.p2phost.core.exception.P2pHostException;
import org.apache.commons.codec.binary.Hex;

public class ClientTest {

    public static void main(String[] args) throws P2pHostException {
        YottaP2P.start(5555, "5KQKydL7TuRwjzaFSK4ezH9RUXWuYHW1yYDp5CmQfsfTuu9MBLZ");
        String[] serverAddrs = {"/p2p-circuit",
            "/ip4/152.136.11.202/tcp/9999"};
        YottaP2P.connect("16Uiu2HAm4ejSpUiVYEYc2pCk7RUa3ScdswM6cXGwzTZziSKcAYwi", serverAddrs);
        UploadShardReq req = new UploadShardReq();
        req.setSHARDID(5);
        req.setBPDID(1);
        req.setBPDSIGN("aa".getBytes());
        req.setDAT("bb".getBytes());
        req.setUSERSIGN("cc".getBytes());
        req.setVBI(2);
        req.setVHF("dd".getBytes());
        byte[] result1 = SerializationUtil.serialize(req);
        String ss1 = Hex.encodeHexString(result1);
        System.out.println(ss1);

        byte[] ret = YottaP2P.sendToBPUMsg("16Uiu2HAm44FX3YuzGXJgHMqnyMM5zCzeT6PUoBNZkz66LutfRREM", result1);
        Object obj = SerializationUtil.deserialize(ret);
        if (obj instanceof UploadShardResp) {
            UploadShardResp resp = (UploadShardResp) obj;
            System.out.println(resp.getRES());
        } else {
            ServiceException se = (ServiceException) obj;
            System.out.println(se.getErrorCode());
        }

        YottaP2P.disconnect("16Uiu2HAm44FX3YuzGXJgHMqnyMM5zCzeT6PUoBNZkz66LutfRREM");
    }
}
