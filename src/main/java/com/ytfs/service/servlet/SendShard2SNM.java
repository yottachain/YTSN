package com.ytfs.service.servlet;

import static com.ytfs.service.ServerConfig.SENDSHARDTHREAD;
import com.ytfs.service.utils.GlobleThreadPool;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import org.bson.Document;

public class SendShard2SNM implements Runnable {

    private static final ArrayBlockingQueue<SendShard2SNM> queue;

    static {
        int num = SENDSHARDTHREAD > 255 ? 255 : SENDSHARDTHREAD;
        num = num < 5 ? 5 : num;
        queue = new ArrayBlockingQueue(num);
        for (int ii = 0; ii < num; ii++) {
            queue.add(new SendShard2SNM());
        }
    }

    static void sendShard2SNM(List<Document> ls) throws InterruptedException {
        SendShard2SNM uploader = queue.take();
        GlobleThreadPool.execute(uploader);
    }

    @Override
    public void run() {
        try {

        } finally {
            queue.add(this);
        }
    }

}
