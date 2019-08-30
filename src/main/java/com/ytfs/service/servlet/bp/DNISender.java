package com.ytfs.service.servlet.bp;

import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.packet.bp.AddDNIReq;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DNISender {

    private static AddDNISender[] senders;

    public static final void start() {
        int count = SuperNodeList.getSuperNodeCount();
        senders = new AddDNISender[count];
        for (int ii = 0; ii < count; ii++) {
            AddDNISender sender = AddDNISender.startSender(ii);
            senders[ii] = sender;
        }
    }

    public static final void stop() {
        List<AddDNISender> ls = new ArrayList(Arrays.asList(senders));
        ls.stream().forEach((sender) -> {
            sender.stopSend();
        });
    }

    public static void startSender(byte[] VHF, int nid) {
        AddDNIReq req = new AddDNIReq();
        req.setDni(VHF);
        req.setNodeid(nid);
        SuperNode sn = SuperNodeList.getDNISuperNode(nid);
        senders[sn.getId()].putMessage(req);
    }

}
