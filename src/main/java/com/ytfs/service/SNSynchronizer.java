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

public class SNSynchronizer implements Runnable {

    public static Object[] request(Object req, boolean noExecloacl) throws InterruptedException {
        return request(req, null, noExecloacl);
    }

    public static Object[] request(Object req, String pubkey, boolean noExecloacl) throws InterruptedException {
        SuperNode[] snlist = SuperNodeList.getSuperNodeList();
        Object[] res = new Object[snlist.length];
        AtomicInteger num = new AtomicInteger(0);
        for (SuperNode node : snlist) {
            if (noExecloacl && node.getId() == ServerConfig.superNodeID) {
                continue;
            }
            SNSynchronizer sync = new SNSynchronizer(res, num);
            sync.req = req;
            sync.pubkey = pubkey;
            sync.node = node;
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
    private String pubkey;
    private final Object[] res;
    private final AtomicInteger count;

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
        try {
            if (node.getId() == ServerConfig.superNodeID) {
                Handler handler = HandlerFactory.getHandler(req);
                handler.setPubkey(pubkey);
                resp = handler.handle();
            } else {
                if (pubkey != null) {
                    resp = P2PUtils.requestBPU(req, node);
                } else {
                    resp = P2PUtils.requestBP(req, node);
                }
            }
        } catch (ServiceException se) {
            resp = se;
        } catch (Throwable ex) {
            resp = new ServiceException(SERVER_ERROR);
        }
        onResponse(resp, node.getId());
    }
}
