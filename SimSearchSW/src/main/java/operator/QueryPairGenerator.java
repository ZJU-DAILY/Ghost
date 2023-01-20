package operator;

import entity.QueryPair;
import entity.QueryTraInfo;
import entity.TraPoint;
import entity.Trajectory;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.awt.*;

public class QueryPairGenerator extends KeyedCoProcessFunction<Long, QueryTraInfo, TraPoint, QueryPair> {

    public long timeWindow;

    public QueryPairGenerator(long timeWindow) {
        this.timeWindow = timeWindow;
    }

    //缓存query来之前的tra
    private ValueState<Trajectory> traState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        traState = getRuntimeContext().getState(
                new ValueStateDescriptor<Trajectory>("traState", Trajectory.class, new Trajectory(timeWindow))
        );
    }

    @Override
    public void processElement1(QueryTraInfo queryTraInfo, KeyedCoProcessFunction<Long, QueryTraInfo, TraPoint, QueryPair>.Context ctx, Collector<QueryPair> out) throws Exception {
        Trajectory tra = traState.value();
        if (tra.id == -1) return; //尚未形成
        tra = SerializationUtils.clone(tra);
        out.collect(new QueryPair(queryTraInfo.queryTra, tra, queryTraInfo.info.threshold));
    }

    @Override
    public void processElement2(TraPoint point, KeyedCoProcessFunction<Long, QueryTraInfo, TraPoint, QueryPair>.Context ctx, Collector<QueryPair> out) throws Exception {
        Trajectory tra = traState.value();
        tra.addPoint(point);
        traState.update(tra);
    }
}
