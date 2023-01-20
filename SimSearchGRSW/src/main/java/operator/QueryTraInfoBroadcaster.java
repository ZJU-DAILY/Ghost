package operator;

import entity.QueryTraInfo;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class QueryTraInfoBroadcaster implements FlatMapFunction<QueryTraInfo, QueryTraInfo> {
    public int datasize;
    public int dataSplitScale;

    public QueryTraInfoBroadcaster(int datasize, int dataSplitScale) {
        this.datasize = datasize;
        this.dataSplitScale = dataSplitScale;
    }

    @Override
    public void flatMap(QueryTraInfo queryTraInfo, Collector<QueryTraInfo> out) throws Exception {
        for (long anotherId = 1; anotherId <= datasize; anotherId++) {
            if (!dataScaleFilter(anotherId)) continue;
            out.collect(new QueryTraInfo(queryTraInfo, anotherId));
        }
    }

    public boolean dataScaleFilter(Long id) {
        long mod = id % 5;
        boolean filter = false;
        for (int i = 0; i <= dataSplitScale - 1; i++) {
            filter = filter || (mod == i);
        }
        return filter;
    }
}
