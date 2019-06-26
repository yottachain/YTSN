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

    /**
     * 并行执行
     *
     * @param req
     * @param exclude 该超级节点跳过
     * @return Object[]
     * @throws InterruptedException
     */
    public static Object[] request(Object req, int exclude) throws InterruptedException {
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
                resp = handler.handle();
            } else {
                resp = P2PUtils.requestBP(req, node);
            }
        } catch (ServiceException se) {
            resp = se;
        } catch (Throwable ex) {
            resp = new ServiceException(SERVER_ERROR);
        }
        onResponse(resp, node.getId());
    }
}
