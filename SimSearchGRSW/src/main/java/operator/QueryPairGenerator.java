package operator;

import entity.*;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import tool.GridTool;

import java.util.Set;

public class QueryPairGenerator extends KeyedCoProcessFunction<Long, QueryTraInfo, TraPoint, QueryPair> {

    public double gridLen;
    public long gridRate;
    public long timeWindow;

    public QueryPairGenerator(double gridLen, long gridRate, long timeWindow) {
        this.gridLen = gridLen;
        this.gridRate = gridRate;
        this.timeWindow = timeWindow;
    }

    //缓存query来之前的tra
    private ValueState<Trajectory> traState;
    private MapState<Tuple2<Integer, Integer>, Integer> gridCntMap;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        traState = getRuntimeContext().getState(
                new ValueStateDescriptor<Trajectory>("traState", Trajectory.class, new Trajectory(timeWindow))
        );
        gridCntMap = getRuntimeContext().getMapState(
                new MapStateDescriptor<Tuple2<Integer, Integer>, Integer>("gridCntMap",
                        TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                        }),
                        TypeInformation.of(new TypeHint<Integer>() {
                        }))
        );
    }

    @Override
    public void processElement1(QueryTraInfo info, KeyedCoProcessFunction<Long, QueryTraInfo, TraPoint, QueryPair>.Context ctx, Collector<QueryPair> out) throws Exception {
        Trajectory tra = traState.value();
        if (tra.id == -1) return; //尚未形成
        tra = SerializationUtils.clone(tra);

        if (info.queryTra.id == tra.id) {
            out.collect(new QueryPair(info.queryTra, info.queryTra, info.info.threshold));
            return;
        }
        for (Tuple2<Integer, Integer> gridID : info.relavantGridIDs) {
            if (gridCntMap.contains(gridID)) {
                out.collect(new QueryPair(info.queryTra, tra, info.info.threshold));
                return;
            }
        }
    }

    @Override
    public void processElement2(TraPoint point, KeyedCoProcessFunction<Long, QueryTraInfo, TraPoint, QueryPair>.Context ctx, Collector<QueryPair> out) throws Exception {
        Trajectory tra = traState.value();

        //新增grid
        Tuple2<Integer, Integer> newGridID = GridTool.fromPoint(gridLen, gridRate, point);
        if (!gridCntMap.contains(newGridID)) gridCntMap.put(newGridID, 0);
        gridCntMap.put(newGridID, gridCntMap.get(newGridID) + 1);

        Set<TraPoint> oldPoints = tra.addPoint(point);
        //删去旧grid
        for (TraPoint oldPoint : oldPoints) {
            Tuple2<Integer, Integer> oldGridID = GridTool.fromPoint(gridLen, gridRate, oldPoint);
            if (gridCntMap.contains(oldGridID)) {
                int curcnt = gridCntMap.get(oldGridID);
                curcnt--;
                if (curcnt <= 0) {
                    gridCntMap.remove(oldGridID);
                } else {
                    gridCntMap.put(oldGridID, curcnt);
                }
            }
        }

        traState.update(tra);
    }
}
