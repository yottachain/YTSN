package com.ytfs.service.servlet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ErrorNodeCache {

    private static final long EXPIRED_TIME = 1000 * 60;

    private static final Map<Integer, Long> errIds = new ConcurrentHashMap<>();

    public static void addErrorNode(Integer id) {
        errIds.put(id, System.currentTimeMillis());
    }

    public static int[] getErrorIds() {
        List<Map.Entry<Integer, Long>> ents = new ArrayList(errIds.entrySet());
        for (Map.Entry<Integer, Long> ent : ents) {
            if (System.currentTimeMillis() - ent.getValue() > EXPIRED_TIME) {
                errIds.remove(ent.getKey());
            }
        }
        List<Integer> idlist = new ArrayList(errIds.keySet());
        int[] ids = new int[idlist.size()];
        for (int ii = 0; ii < ids.length; ii++) {
            ids[ii] = idlist.get(ii);
        }
        return ids;
    }

}
