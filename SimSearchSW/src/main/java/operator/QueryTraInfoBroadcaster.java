package operator;

import entity.QueryTraInfo;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class QueryTraInfoBroadcaster implements FlatMapFunction<QueryTraInfo, QueryTraInfo> {
    public int datasize;

    public QueryTraInfoBroadcaster(int datasize) {
        this.datasize = datasize;
    }

    @Override
    public void flatMap(QueryTraInfo queryTraInfo, Collector<QueryTraInfo> out) throws Exception {
        for (int anotherId = 1; anotherId <= datasize; anotherId++) {
            out.collect(new QueryTraInfo(queryTraInfo, anotherId));
        }
    }
}
