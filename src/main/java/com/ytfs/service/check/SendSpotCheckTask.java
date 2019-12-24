package com.ytfs.service.check;

import com.ytfs.common.conf.MessageWriter;
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
//作废
public class SendSpotCheckTask extends Thread {

    private static final Logger LOG = Logger.getLogger(SendSpotCheckTask.class);

    private boolean exit = false;
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
        List<Node> nlist = null;
        List<SpotCheckList> sc = null;
        while (!exit) {
            try {
                long time = System.currentTimeMillis();
                long min = time % 600000L;
                try {
                    if (min < 60000 * 3) {
                        sc = YottaNodeMgmt.getSpotCheckList();
                        if (sc != null && !sc.isEmpty()) {
                            nlist = YottaNodeMgmt.getSTNodes(sc.size());
                            if (nlist.size() != sc.size()) {
                                throw new Exception("getSTNodes return count:" + nlist.size() + "!=" + sc.size());
                            }
                            LOG.info("Query returns " + sc.size() + " tasks.");
                        }
                    }
                } catch (Throwable t) {
                    LOG.error("Get SpotCheckList ERR:" + t.getMessage());
                    sleep(30000);
                    continue;
                }
                if (sc != null && !sc.isEmpty() && nlist != null && !nlist.isEmpty()) {
                    for (SpotCheckList scheck : sc) {
                        try {
                            sendTask(scheck, nlist.remove(0));
                        } catch (Throwable t) {
                            LOG.error("Send task [" + scheck.getTaskID() + "] err: " + t.getMessage());
                        }
                    }
                    sc.clear();
                    sleep(60000 * 3);
                }
                sleep(30000);
            } catch (InterruptedException ex) {
                break;
            } catch (Throwable ne) {
                try {
                    sleep(15000);
                } catch (InterruptedException ex) {
                    break;
                }
            }
        }
    }

    private void sendTask(SpotCheckList scheck, Node n) throws NodeMgmtException, ServiceException {
        SpotCheckTaskList mytask = new SpotCheckTaskList();
        mytask.setSnid(ServerConfig.superNodeID);
        mytask.setTaskId(scheck.getTaskID());
        mytask.setTaskList(new ArrayList());
        List<SpotCheckTask> ls = scheck.getTaskList();
        ls.stream().map((st) -> {
            com.ytfs.service.packet.SpotCheckTask myst = new com.ytfs.service.packet.SpotCheckTask();
            myst.setId(st.getId());
            myst.setNodeId(st.getNodeID());
            myst.setAddr(st.getAddr());
            byte[] vni = Base64.decodeBase64(st.getVni());
            if (vni.length > 16) {
                byte[] VHF = new byte[16];
                System.arraycopy(vni, vni.length - 16, VHF, 0, 16);
                myst.setVHF(VHF);
            } else {
                myst.setVHF(vni);
            }
            return myst;
        }).forEach((myst) -> {
            mytask.getTaskList().add(myst);
        });

        MessageWriter.write(mytask);
        LOG.info("Send task [" + mytask.getTaskId() + "] to " + n.getId() + ":" + P2PUtils.getAddrString(n.getAddrs()));
        //P2PUtils.requestNode(mytask, n);
        LOG.info("Send task [" + mytask.getTaskId() + "] OK!");
    }

}
