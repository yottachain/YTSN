package com.ytfs.service.check;

import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.SpotCheckTaskList;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.exception.NodeMgmtException;
import io.yottachain.nodemgmt.core.vo.Node;
import io.yottachain.nodemgmt.core.vo.SpotCheckList;
import io.yottachain.nodemgmt.core.vo.SpotCheckTask;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

public class SendSpotCheckTask extends Thread {

    private static final Logger LOG = Logger.getLogger(SendSpotCheckTask.class);
    private List<SpotCheckList> sc;
    private boolean exit = false;
    private final long inteval = 1000 * 60 * 60;

    private static SendSpotCheckTask instance;

    public static synchronized void startUp() {
        if (instance == null) {
            instance = new SendSpotCheckTask();
            instance.start();
        }
    }

    public static synchronized void shutdown() {
        if (instance != null) {
            instance.exit = true;
            instance.interrupt();
        }
    }

    @Override
    public void run() {
        LOG.info("SpotCheckTask distributor startup...");
        try {
            sleep(1000 * 60);
        } catch (InterruptedException ex) {
            return;
        }
        while (!exit) {
            try {
                if (sc == null || sc.isEmpty()) {
                    try {
                        sc = YottaNodeMgmt.getSpotCheckList();
                    } catch (Throwable t) {
                        LOG.error("Get SpotCheckList ERR:" + t.getMessage());
                        sleep(60000);
                        continue;
                    }
                    LOG.info("Query returns " + sc.size() + " tasks.");
                }
                if (!sc.isEmpty()) {
                    SpotCheckList scheck = sc.get(0);
                    sendTask(scheck);
                    sc.remove(scheck);
                }
                if (sc.isEmpty()) {
                    sleep(inteval);
                }
            } catch (InterruptedException ex) {
                break;
            } catch (Throwable ne) {
                LOG.error("ERR:", ne);
                try {
                    sleep(15000);
                } catch (InterruptedException ex) {
                    break;
                }
            }
        }
    }

    private void sendTask(SpotCheckList scheck) throws NodeMgmtException, ServiceException {
        SpotCheckTaskList mytask = new SpotCheckTaskList();
        mytask.setSnid(ServerConfig.superNodeID);
        mytask.setTaskId(scheck.getTaskID());
        mytask.setTaskList(new ArrayList());
        List<SpotCheckTask> ls = scheck.getTaskList();
        for (SpotCheckTask st : ls) {
            com.ytfs.service.packet.SpotCheckTask myst = new com.ytfs.service.packet.SpotCheckTask();
            myst.setId(st.getId());
            myst.setNodeId(st.getNodeID());
            myst.setAddr(st.getAddr());
            byte[] vni = Base64.decodeBase64(st.getVni());
            if (vni.length > 32) {
                byte[] VHF = new byte[32];
                System.arraycopy(vni, vni.length - 32, VHF, 0, 32);
                myst.setVHF(VHF);
            } else {
                myst.setVHF(vni);
            }
            mytask.getTaskList().add(myst);
        }
        Node n = YottaNodeMgmt.getSTNode();
        LOG.info("Send task [" + mytask.getTaskId() + "] to " + P2PUtils.getAddrString(n.getAddrs()));
        P2PUtils.requestNode(mytask, n);
    }
}
