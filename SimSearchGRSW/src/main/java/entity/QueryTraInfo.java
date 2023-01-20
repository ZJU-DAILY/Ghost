package entity;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Set;

public class QueryTraInfo implements Serializable {
    public Trajectory queryTra;
    public QueryInfo info;
    public Set<Tuple2<Integer,Integer>> relavantGridIDs;
    public long anotherTraId = -1;

    public QueryTraInfo() {
    }

    public QueryTraInfo(Trajectory tra, QueryInfo info) {
        this.queryTra = tra;
        this.info = info;
    }

    //for broadcast
    public QueryTraInfo(QueryTraInfo another, long anotherTraId) {
        queryTra = another.queryTra;
        info = another.info;
        relavantGridIDs = another.relavantGridIDs;
        this.anotherTraId = anotherTraId;
    }
}
