package com.ytfs.service;

import com.ytfs.common.GlobleThreadPool;
import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.servlet.Handler;
import com.ytfs.service.servlet.HandlerFactory;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

public class SNSynchronizer implements Runnable {

    private static final Logger LOG = Logger.getLogger(SNSynchronizer.class);

    public static void ayncRequest(Object req, int exclude, int retrytime) throws InterruptedException {
        SuperNode[] snlist = SuperNodeList.getSuperNodeList();
        Object[] res = new Object[snlist.length];
        AtomicInteger num = new AtomicInteger(0);
        for (SuperNode node : snlist) {
            if (node.getId() == exclude) {
                num.incrementAndGet();
                continue;
            }
            SNSynchronizer sync = new SNSynchronizer(res, num);
            sync.req = req;
            sync.node = node;
            sync.retryTimes = retrytime;
            GlobleThreadPool.execute(sync);
        }
    }

    public static Object[] syncRequest(Object req, int exclude) throws InterruptedException {
        return syncRequest(req, exclude, 0);
    }

    /**
     * 并行执行
     *
     * @param req
     * @param exclude 该超级节点跳过int retrytime
     * @param retrytime
     * @return Object[]
     * @throws InterruptedException
     */
    public static Object[] syncRequest(Object req, int exclude, int retrytime) throws InterruptedException {
        SuperNode[] snlist = SuperNodeList.getSuperNodeList();
        Object[] res = new Object[snlist.length];
        AtomicInteger num = new AtomicInteger(0);
        for (SuperNode node : snlist) {
            if (node.getId() == exclude) {
                num.incrementAndGet();
                continue;
            }
            SNSynchronizer sync = new SNSynchronizer(res, num);
            sync.req = req;
            sync.node = node;
            sync.retryTimes = retrytime;
            GlobleThreadPool.execute(sync);
        }
        synchronized (res) {
            while (num.get() != snlist.length) {
                res.wait(1000 * 15);
            }
        }
        return res;
    }

    private Object req;
    private SuperNode node;
    private final Object[] res;
    private final AtomicInteger count;
    private int retryTimes = 0;

    private SNSynchronizer(Object[] res, AtomicInteger num) {
        this.res = res;
        this.count = num;
    }

    private void onResponse(Object obj, int index) {
        synchronized (res) {
            res[index] = obj;
            count.incrementAndGet();
            res.notify();
        }
    }

    @Override
    public void run() {
        Object resp;
        while (true) {
            try {
                if (node.getId() == ServerConfig.superNodeID) {
                    Handler handler = HandlerFactory.getHandler(req);
                    String pubkey = node.getPubkey();
                    if (pubkey.startsWith("EOS")) {
                        pubkey = pubkey.substring(3);
                    }
                    handler.setPubkey(pubkey);
                    resp = handler.handle();
                } else {
                    resp = P2PUtils.requestBP(req, node);
                    LOG.debug("Sync " + req.getClass().getSimpleName() + " to " + node.getId());
                }
                break;
            } catch (ServiceException se) {
                LOG.error("Sync " + req.getClass().getSimpleName() + " to " + node.getId() + " ERR.");
                resp = se;
                if (retryTimes == 0) {
                    break;
                } else {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ex) {
                    }
                }
                retryTimes--;
            } catch (Throwable ex) {
                LOG.error("Sync " + req.getClass().getSimpleName() + " to " + node.getId() + " ERR:" + ex.getMessage());
                resp = new ServiceException(SERVER_ERROR);
                if (retryTimes == 0) {
                    break;
                } else {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                    }
                }
                retryTimes--;
            }
        }
        onResponse(resp, node.getId());
    }
}
