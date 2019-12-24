package com.ytfs.service.check;

import com.ytfs.common.GlobleThreadPool;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.packet.TaskDispatchList;
import com.ytfs.service.servlet.bp.TaskListHandler;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.log4j.Logger;

public class SendRebuildTask implements Runnable {

    private static final Logger LOG = Logger.getLogger(SendRebuildTask.class);

    private static final ArrayBlockingQueue<SendRebuildTask> queue;
    private static final ArrayBlockingQueue<SendRebuildTask> localqueue;

    static {
        int num = SuperNodeList.getSuperNodeCount();
        queue = new ArrayBlockingQueue(num);
        for (int ii = 0; ii < num; ii++) {
            queue.add(new SendRebuildTask());
        }
        localqueue = new ArrayBlockingQueue(num);
        for (int ii = 0; ii < num; ii++) {
            localqueue.add(new SendRebuildTask());
        }
    }

    public static void startLocalSender(int snnode, TaskDispatchList req) {
        try {
            SendRebuildTask sender = localqueue.take();
            sender.req = req;
            sender.snnode = snnode;
            sender.me = localqueue;
            GlobleThreadPool.execute(sender);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    public static void startRemoteSender(int snnode, TaskDispatchList req) {
        try {
            SendRebuildTask sender = queue.take();
            sender.req = req;
            sender.snnode = snnode;
            sender.me = queue;
            GlobleThreadPool.execute(sender);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private TaskDispatchList req;
    private int snnode;
    private ArrayBlockingQueue<SendRebuildTask> me;

    @Override
    public void run() {
        try {
            for (int ii = 0; ii < 3; ii++) {
                try {
                    SuperNode sn = SuperNodeList.getSuperNode(snnode);
                    TaskListHandler.taskDispatchCall(req, sn);
                    break;
                } catch (Throwable r) {
                    LOG.error("Send rebuild tasks ERR:" + r.getMessage());
                    try {
                        Thread.sleep(30000);
                    } catch (InterruptedException ex) {
                        break;
                    }
                }
            }
        } finally {
            me.add(this);
        }
    }
}
