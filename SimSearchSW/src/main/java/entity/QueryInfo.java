package entity;

import java.io.Serializable;

public class QueryInfo implements Serializable {
    public long queryTraId;
    public double threshold;

    public QueryInfo(long queryId, double threshold) {
        this.queryTraId = queryId;
        this.threshold = threshold;
    }
}
