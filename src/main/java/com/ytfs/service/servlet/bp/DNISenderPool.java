package com.ytfs.service.servlet.bp;

import com.ytfs.common.GlobleThreadPool;
import static com.ytfs.common.conf.ServerConfig.sendShardInterval;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.dao.CacheBaseAccessor;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import org.bson.Document;

public class DNISenderPool extends Thread {

    private static final Logger LOG = Logger.getLogger(DNISenderPool.class);
    private static DNISenderPool me;

    public static final void startup() {
        if (me == null) {
            me = new DNISenderPool();
            me.start();
        }
    }

    public static final void shutdown() {
        if (me != null) {
            me.interrupt();
            me = null;
        }
    }

    @Override
    public void run() {
        int max_size = SuperNodeList.getSuperNodeCount() * 200;
        while (!this.isInterrupted()) {
            try {
                int count = 0;
                List<DNISender> sendList = new ArrayList();
                Map<SuperNode, List<Document>> map = CacheBaseAccessor.queryDNI(max_size);
                Set<Map.Entry<SuperNode, List<Document>>> set = map.entrySet();
                for (Map.Entry<SuperNode, List<Document>> ent : set) {
                    DNISender sender = new DNISender(ent.getKey(), ent.getValue(), sendList);
                    sendList.add(sender);
                    count = count + ent.getValue().size();
                }
                sendList.forEach((send) -> {
                    GlobleThreadPool.execute(send);
                });
                synchronized (sendList) {
                    while (!sendList.isEmpty()) {
                        sendList.wait(15000);
                    }
                }
                if (count < max_size) {
                    sleep(1000 * 60);
                } else {
                    if (sendShardInterval > 0) {
                        sleep(sendShardInterval);
                    }
                }
            } catch (InterruptedException ie) {
                break;
            } catch (Throwable t) {
                LOG.error("ERR:" + t.getMessage());
                try {
                    sleep(1000 * 10);
                } catch (InterruptedException ex) {
                    break;
                }
            }
        }
    }

}
