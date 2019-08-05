package com.ytfs.service.dao.sync;

import com.ytfs.service.dao.ShardMeta;
import java.util.List;

public class ShardInsertLog {

    private List<ShardMeta> shards;

    /**
     * @return the shards
     */
    public List<ShardMeta> getShards() {
        return shards;
    }

    /**
     * @param shards the shards to set
     */
    public void setShards(List<ShardMeta> shards) {
        this.shards = shards;
    }

}
