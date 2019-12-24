package com.ytfs.service.check;

import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.packet.TaskDispatchList;
import java.util.concurrent.ArrayBlockingQueue;

public class QueryRebuildTaskQueue extends Thread {

    private static final ArrayBlockingQueue<TaskDispatchList> queue;

    static {
        int num = SuperNodeList.getSuperNodeCount() * 2;
        queue = new ArrayBlockingQueue(num);
    }

    public static boolean putRequest(TaskDispatchList req) {
        return queue.offer(req);
    }

    @Override
    public void run() {
        while (!this.isInterrupted()) {
            try {
                TaskDispatchList req = queue.take();
                SendRebuildTask.startLocalSender(ServerConfig.superNodeID, req);
            } catch (InterruptedException r) {
                break;
            } catch (Throwable e) {
            }
        }

    }

}
