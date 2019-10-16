package com.ytfs.service.check;

import com.ytfs.common.GlobleThreadPool;
import static com.ytfs.common.conf.ServerConfig.REBULIDTHREAD;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.packet.TaskDispatchList;
import com.ytfs.service.servlet.bp.TaskListHandler;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.log4j.Logger;

public class SendRebuildTask implements Runnable {

    private static final Logger LOG = Logger.getLogger(SendRebuildTask.class);

    private static final ArrayBlockingQueue<SendRebuildTask> queue;

    static {
        int num = REBULIDTHREAD > 255 ? 255 : REBULIDTHREAD;
        num = num < 5 ? 5 : num;
        queue = new ArrayBlockingQueue(num);
        for (int ii = 0; ii < num; ii++) {
            queue.add(new SendRebuildTask());
        }
    }

    public static void startSender(int snnode, TaskDispatchList req) {
        try {
            SendRebuildTask sender = queue.take();
            sender.req = req;
            sender.snnode = snnode;
            GlobleThreadPool.execute(sender);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
    private TaskDispatchList req;
    private int snnode;

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
                        Thread.sleep(15000);
                    } catch (InterruptedException ex) {
                        break;
                    }
                }
            }
        } finally {
            queue.add(this);
        }
    }
}
