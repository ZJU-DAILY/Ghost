package operator;

import entity.QueryPair;
import org.apache.flink.api.common.functions.ReduceFunction;

public class ResultReducer implements ReduceFunction<QueryPair> {
    @Override
    public QueryPair reduce(QueryPair value1, QueryPair value2) throws Exception {
        return new QueryPair(value1, value2);
    }
}
