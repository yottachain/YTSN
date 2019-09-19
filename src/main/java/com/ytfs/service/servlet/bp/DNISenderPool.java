package com.ytfs.service.servlet.bp;

import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.packet.bp.UpdateDNIReq;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DNISenderPool {

    private static DNISender[] senders;

    public static final void start() {
        int count = SuperNodeList.getSuperNodeCount();
        senders = new DNISender[count];
        for (int ii = 0; ii < count; ii++) {
            DNISender sender = DNISender.startSender(ii);
            senders[ii] = sender;
        }
    }

    public static final void stop() {
        List<DNISender> ls = new ArrayList(Arrays.asList(senders));
        ls.stream().forEach((sender) -> {
            sender.stopSend();
        });
    }

    public static void startSender(byte[] VHF, int nid, boolean delete) {
        UpdateDNIReq req = new UpdateDNIReq();
        req.setDni(VHF);
        req.setNodeid(nid);
        req.setDelete(delete);
        SuperNode sn = SuperNodeList.getDNISuperNode(nid);
        senders[sn.getId()].putMessage(req);
    }

}
