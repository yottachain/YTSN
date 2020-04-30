package com.ytfs.service.cost;

import com.mongodb.client.FindIterable;
import com.ytfs.common.conf.ServerConfig;
import com.ytfs.common.eos.EOSClient;
import com.ytfs.common.node.SuperNodeList;
import static com.ytfs.service.ServiceWrapper.FEESUM;
import com.ytfs.service.dao.CacheBaseAccessor;
import com.ytfs.service.dao.MongoSource;
import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserAccessor;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import org.apache.log4j.Logger;
import org.bson.Document;

public class UserFeeStat extends Thread {

    private static final Logger LOG = Logger.getLogger(UserFeeStat.class);

    private static UserFeeStat instance;

    public static synchronized void startUp() {
        if (instance == null) {
            instance = new UserFeeStat();
            instance.start();
        }
    }

    public static synchronized void shutdown() {
        if (instance != null) {
            instance.interrupt();
        }
    }

    private void iterate() {
        Document fields = new Document("_id", 1);
        fields.append("nextCycle", 1);
        fields.append("username", 1);
        FindIterable<Document> it = MongoSource.getUserCollection().find().projection(fields);
        for (Document doc : it) {
            int userid = doc.getInteger("_id");
            SuperNode sn = SuperNodeList.getUserSuperNode(userid);
            if (sn.getId() != ServerConfig.superNodeID) {
                continue;
            }
            long nextCycle = CacheBaseAccessor.getUserSumTime(userid);
            if (System.currentTimeMillis() - nextCycle < ServerConfig.CostSumCycle) {
                continue;
            }
            UserFileIterator userFileIterator = new UserFileIterator(userid);
            userFileIterator.iterate();
            long usedSpace = userFileIterator.getUsedSpace();
            setCycleFee(usedSpace, userid, doc.getString("username"));
        }
    }

    private void setCycleFee(long usedSpace, int userid, String username) {
        long costPerCycle = ServerConfig.unitCycleCost * usedSpace / ServerConfig.unitSpace;
        LOG.info("User " + userid + " Statistics complete! usedSpace:" + usedSpace + ",costPerCycle:" + costPerCycle);
        try {
            if (costPerCycle > 0) {
                long oldCostPerCycle = UserAccessor.getUserCostPerCycle(userid);
                if (costPerCycle == oldCostPerCycle) {
                    LOG.info("User " + userid + " not need to set costPerCycle,old costPerCycle:" + costPerCycle);
                } else {
                    EOSClient.setUserFee(costPerCycle, username);
                    UserAccessor.updateUser(userid, costPerCycle);
                    LOG.info("User " + userid + " set costPerCycle:" + costPerCycle + ", usedSpace:" + usedSpace);
                }
            }
            CacheBaseAccessor.setUserSumTime(userid);
        } catch (Throwable r) {
            LOG.error("Set costPerCycle ERR:" + r.getMessage());
        }
    }

    @Override
    public void run() {
        while (!this.isInterrupted()) {
            try {
                Thread.sleep(1000 * 60 * 30);
                if (!SuperNodeList.isActive()) {
                    iterate();
                } else {
                    if (FEESUM) {
                        iterate();
                    }
                }
                Thread.sleep(1000 * 60 * 30);
            } catch (InterruptedException ex) {
                break;
            } catch (Throwable e) {
                LOG.error("", e);
                try {
                    Thread.sleep(1000 * 15);
                } catch (InterruptedException ex) {
                    break;
                }
            }
        }

    }
}
