package com.ytfs.service.dao;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.ytfs.common.ServiceException;
import com.ytfs.service.dao.sync.LogMessage;
import static com.ytfs.service.dao.sync.LogMessageCode.Op_User_New;
import com.ytfs.service.packet.bp.UserSpace;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

public class UserAccessor {

    private static final Logger LOG = Logger.getLogger(UserAccessor.class);

    public static Document total() {
        List<Document> ops = new ArrayList();
        Document all = new Document(new Document("_id", "ALL"));
        all.append("usedspace", new Document("$sum", "$usedspace"));
        all.append("spaceTotal", new Document("$sum", "$spaceTotal"));
        all.append("fileTotal", new Document("$sum", "$fileTotal"));
        Document sum = new Document("$group", all);
        ops.add(sum);
        Document document = MongoSource.getUserCollection().aggregate(ops).first();
        return document;
    }

    public static long getUserCount() throws ServiceException {
        return MongoSource.getUserCollection().countDocuments();
    }

    public static void updateUser(int uid, long costPerCycle) {
        Bson bson = Filters.eq("_id", uid);
        Document doc = new Document("costPerCycle", costPerCycle);
        doc.append("nextCycle", System.currentTimeMillis());
        Document update = new Document("$set", doc);
        MongoSource.getUserCollection().updateOne(bson, update);
    }

    public static void updateUser(int uid, long usedSpace, long fileTotal, long spaceTotal) {
        Bson bson = Filters.eq("_id", uid);
        Document doc = new Document("usedspace", usedSpace);
        doc.append("fileTotal", fileTotal);
        doc.append("spaceTotal", spaceTotal);
        Document update = new Document("$inc", doc);
        MongoSource.getUserCollection().updateOne(bson, update);
    }

    public static List<UserSpace> getUserList(int lastId, int limit) {
        List<UserSpace> ls = new ArrayList();
        if (limit < 10) {
            limit = 10;
        }
        if (limit > 1000) {
            limit = 1000;
        }
        Bson filter = Filters.gt("_id", lastId);
        Document fields = new Document("_id", 1);
        fields.append("username", 1);
        fields.append("spaceTotal", 1);
        Bson sort = new Document("_id", 1);
        FindIterable<Document> it = MongoSource.getUserCollection().find(filter).projection(fields).sort(sort).limit(limit);
        for (Document doc : it) {
            UserSpace space = new UserSpace();
            space.setUserId(doc.getInteger("_id"));
            space.setUserName(doc.getString("username"));
            space.setSpaceTotal(doc.getLong("spaceTotal"));
            if (space.getSpaceTotal() > 0) {
                ls.add(space);
            }
        }
        return ls;
    }

    public static User getUser(int uid) {
        Bson bson = Filters.eq("_id", uid);
        Document document = MongoSource.getUserCollection().find(bson).first();
        if (document == null) {
            return null;
        } else {
            return new User(document);
        }
    }

    public static User getUser(String username) {
        Bson bson = Filters.eq("username", username);
        Document document = MongoSource.getUserCollection().find(bson).first();
        if (document == null) {
            return null;
        } else {
            return new User(document);
        }
    }

    public static void addUser(User user) {
        MongoSource.getUserCollection().insertOne(user.toDocument());
        if (MongoSource.getProxy() != null) {
            LogMessage log = new LogMessage(Op_User_New, user);
            MongoSource.getProxy().post(log);
            LOG.debug("DBlog: sync user " + user.getUsername());
        }
    }

}
