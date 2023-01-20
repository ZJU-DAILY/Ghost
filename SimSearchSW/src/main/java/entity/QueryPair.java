package entity;

import java.io.Serializable;

public class QueryPair implements Serializable {

    public Trajectory queryTra;
    public Trajectory anotherTra;
    public double threshold;

    public double similarityDistance = Double.MAX_VALUE;
    public long startTimestamp = Long.MAX_VALUE;
    public long endTimestamp = 0;

    //for reduce
    public boolean reduced = false;
    public int numSimilarTra = 0;
    public int numTotalTra = 1;


    public QueryPair(Trajectory queryTra, Trajectory anotherTra, double threshold) {
        this.queryTra = queryTra;
        this.anotherTra = anotherTra;
        this.threshold = threshold;
    }

    //for reduce
    public QueryPair(QueryPair value1, QueryPair value2) {
        this.reduced = true;
        this.queryTra = value1.queryTra;
        this.startTimestamp = Math.min(value1.startTimestamp, value2.startTimestamp);
        this.endTimestamp = Math.max(value1.endTimestamp, value2.endTimestamp);
        this.numTotalTra = value1.numTotalTra + value2.numTotalTra;
        this.numSimilarTra = value1.numSimilarTra + value2.numSimilarTra;
    }

    @Override
    public String toString() {
        long time = endTimestamp - startTimestamp;
        return String.format("queryId=%d,similar=%d(total=%d),start=%d,end=%d,time(ms)=%d\n", queryTra.id, numSimilarTra, numTotalTra, startTimestamp, endTimestamp, time);
    }
}
