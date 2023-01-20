package entity;

import java.io.Serializable;

public class QueryTraInfo implements Serializable {
    public Trajectory queryTra;
    public QueryInfo info;
    public long anotherTraId = -1;

    public QueryTraInfo(Trajectory tra, QueryInfo info) {
        this.queryTra = tra;
        this.info = info;
    }

    //for broadcast
    public QueryTraInfo(QueryTraInfo another, long anotherTraId) {
        queryTra = another.queryTra;
        info = another.info;
        this.anotherTraId = anotherTraId;
    }
}
