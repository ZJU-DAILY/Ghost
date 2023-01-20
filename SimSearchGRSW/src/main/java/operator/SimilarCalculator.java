package operator;

import entity.QueryPair;
import entity.Trajectory;
import measure.Measure;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class SimilarCalculator extends KeyedProcessFunction<Tuple2<Long, Long>, QueryPair, QueryPair> {

    public Measure measure;

    public SimilarCalculator(Measure measure) {
        this.measure = measure;
    }

    @Override
    public void processElement(QueryPair pair, KeyedProcessFunction<Tuple2<Long, Long>, QueryPair, QueryPair>.Context ctx, Collector<QueryPair> out) throws Exception {
        Trajectory queryTra = pair.queryTra;
        Trajectory anotherTra = pair.anotherTra;
        boolean identity = queryTra.id == anotherTra.id;

        pair.similarityDistance = identity ? 0.0 : measure.traDist(queryTra, anotherTra);
        if (!identity && pair.similarityDistance <= pair.threshold) pair.numSimilarTra++;

        out.collect(pair);
    }
}
