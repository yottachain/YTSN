package com.ytfs.service.dao;

import com.mongodb.client.model.Filters;
import com.ytfs.service.dao.sync.LogMessage;
import static com.ytfs.service.dao.sync.LogMessageCode.Op_User_New;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;

public class UserAccessor {

    private static final Logger LOG = Logger.getLogger(UserAccessor.class);

    public static Document total() {
        List<Document> ops = new ArrayList();
        Document all = new Document(new Document("_id", "ALL"));
        all.append("usedspace", new Document("$sum", "$usedspace"));
        all.append("spaceTotal", new Document("$sum", "$spaceTotal"));
        all.append("fileTotal", new Document("$sum", "$fileTotal"));
        all.append("costPerCycle", new Document("$sum", "$costPerCycle"));
        Document sum = new Document("$group", all);
        ops.add(sum);
        Document document = MongoSource.getUserCollection().aggregate(ops).first();
        return document;
    }

    public static Document userTotal(int uid) throws Exception {
        Bson bson = Filters.eq("_id", uid);
        Document fields = new Document("usedspace", 1);
        fields.append("fileTotal", 1);
        fields.append("spaceTotal", 1);
        fields.append("costPerCycle", 1);
        Document document = MongoSource.getUserCollection().find(bson).projection(fields).first();
        if (document == null) {
            throw new Exception("User '" + uid + "' doesn't exist");
        } else {
            return document;
        }
    }

    public static void updateUser(int uid, long costPerCycle) {
        Bson bson = Filters.eq("_id", uid);
        Document doc = new Document("costPerCycle", costPerCycle);
        Document update = new Document("$inc", doc);
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
