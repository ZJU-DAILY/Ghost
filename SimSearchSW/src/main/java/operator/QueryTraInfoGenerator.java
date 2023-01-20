package operator;

import entity.QueryInfo;
import entity.QueryTraInfo;
import entity.TraPoint;
import entity.Trajectory;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class QueryTraInfoGenerator extends KeyedCoProcessFunction<Long, TraPoint, QueryInfo, QueryTraInfo> {

    public long timeWindow;

    public QueryTraInfoGenerator(long timeWindow) {
        this.timeWindow = timeWindow;
    }

    //缓存在query到来之前tra的状态
    private ValueState<Trajectory> traState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        traState = getRuntimeContext().getState(
                new ValueStateDescriptor<Trajectory>("traState", Trajectory.class, new Trajectory(timeWindow))
        );
    }

    @Override
    public void processElement1(TraPoint point, KeyedCoProcessFunction<Long, TraPoint, QueryInfo, QueryTraInfo>.Context ctx, Collector<QueryTraInfo> out) throws Exception {
        Trajectory tra = traState.value();
        tra.addPoint(point);
        traState.update(tra);
    }

    @Override
    public void processElement2(QueryInfo info, KeyedCoProcessFunction<Long, TraPoint, QueryInfo, QueryTraInfo>.Context ctx, Collector<QueryTraInfo> out) throws Exception {
        Trajectory tra = traState.value();
        if (tra.id == -1) {
            System.out.println("invalid query");
            return;
        }
        tra = SerializationUtils.clone(tra);
        out.collect(new QueryTraInfo(tra, info));
    }
}
