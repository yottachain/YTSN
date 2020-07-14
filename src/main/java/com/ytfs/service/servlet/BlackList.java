package com.ytfs.service.servlet;

import static com.ytfs.common.ServiceErrorCode.DN_IN_BLACKLIST;
import com.ytfs.common.ServiceException;
import com.ytfs.common.node.NodeManager;
import com.ytfs.service.packet.UploadShardRes;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.Node;
import java.util.ArrayList;
import java.util.List;

public class BlackList {

    private static List<Integer> blacklists = new ArrayList();
    private static final List<Integer> sign = new ArrayList();

    public static void init() {
        sign.add(-1);
        getter.setDaemon(true);
        getter.start();
    }

    private static final Thread getter = new Thread() {
        @Override
        public void run() {
            for (;;) {
                try {
                    query();
                    Thread.sleep(1000 * 60 * 5);
                } catch (Exception e) {
                    try {
                        Thread.sleep(15000);
                    } catch (InterruptedException ex) {
                    }
                }
            }
        }
    };

    private synchronized static void setBlackList(List<Integer> ls) {
        blacklists = ls;
    }

    public synchronized static List<Integer> getBlackList() {
        return blacklists;
    }

    private static void query() throws NodeMgmtException {
        List<Integer> list = new ArrayList();
        List<Node> ls = NodeManager.getNode(sign);
        ls.forEach((n) -> {
            list.add(n.getId());
        });
        setBlackList(list);
    }

}
