package com.ytfs.service.servlet.bp;

import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.ServerConfig;
import static com.ytfs.common.conf.ServerConfig.SENDDNI_QUEUE;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.dao.DNICacheAccessor;
import com.ytfs.service.packet.bp.AddDNIMutiReq;
import com.ytfs.service.packet.bp.AddDNIReq;
import io.jafka.jeos.util.Base58;
import io.yottachain.nodemgmt.YottaNodeMgmt;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

public class AddDNISender extends Thread {

    private static final Logger LOG = Logger.getLogger(AddDNISender.class);

    private final ArrayBlockingQueue<AddDNIReq> queue;
    private final int snId;

    public AddDNISender(int snId) {
        int num = SENDDNI_QUEUE > 5000 ? 5000 : SENDDNI_QUEUE;
        num = num < 5 ? 5 : num;
        queue = new ArrayBlockingQueue(num);
        this.snId = snId;
    }

    public static AddDNISender startSender(int sid) {
        AddDNISender sender = new AddDNISender(sid);
        sender.start();
        return sender;
    }

    public void stopSend() {
        this.interrupt();
        try {
            this.join(15000);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    public void putMessage(AddDNIReq req) {
        boolean bool = queue.offer(req);
        if (!bool) {
            DNICacheAccessor.addDNI(snId, req.getNodeid(), req.getDni());
        }
    }

    @Override
    public void run() {
        int maxCount = 200;
        List<AddDNIReq> list = new ArrayList();
        long sleeptime = 1000 * 60 * 60;
        boolean retry = false;
        AddDNIReq req = null;
        LOG.info("DNI sender thread " + snId + " startup....");
        while (!this.isInterrupted()) {
            if (snId == ServerConfig.superNodeID) {
                try {
                    req = queue.take();
                    YottaNodeMgmt.addDNI(req.getNodeid(), req.getDni());
                } catch (InterruptedException e) {
                    break;
                } catch (Throwable r) {
                    LOG.error("InsertDNI " + req.getNodeid() + "-[" + Base58.encode(req.getDni()) + "] ERR:" + r.getMessage());
                }
                continue;
            }
            try {
                if (!retry) {
                    long st = System.currentTimeMillis();
                    req = queue.poll(1, TimeUnit.MINUTES);
                    sleeptime = System.currentTimeMillis() - st;
                }
                if (req != null) {
                    list.add(req);
                }
                if (list.size() >= maxCount) {
                    send(list);
                } else {
                    if (!list.isEmpty()) {
                        if (sleeptime > 1000 * 60 * 3) {
                            send(list);
                        }
                    }
                }
                retry = false;
            } catch (InterruptedException e) {
                break;
            } catch (Throwable r) {
                retry = true;
                try {
                    LOG.error("Send DNI to " + snId + " ERR:" + r.getMessage());
                    sleep(5000);
                } catch (InterruptedException ex) {
                    break;
                }
            }
        }
        if (!list.isEmpty()) {
            try {
                send(list);
            } catch (ServiceException ex) {
                LOG.error("Send DNI to " + snId + " ERR:" + ex.getMessage());
            }
        }
    }

    private void send(List<AddDNIReq> list) throws ServiceException {
        SuperNode sn = SuperNodeList.getSuperNode(snId);
        AddDNIMutiReq mreq = new AddDNIMutiReq();
        mreq.setList(list);
        P2PUtils.requestBP(mreq, sn);
        LOG.info("Send DNI OK,count:" + list.size());
        list.clear();
    }

}
