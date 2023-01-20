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

import java.util.HashSet;
import java.util.Set;

public class QueryTraInfoGenerator extends KeyedCoProcessFunction<Long, TraPoint, QueryInfo, QueryTraInfo> {

    public double gridLen;
    public long gridRate;
    public long timeWindow;

    public QueryTraInfoGenerator(double gridLen, long gridRate, long timeWindow) {
        this.gridLen = gridLen;
        this.gridRate = gridRate;
        this.timeWindow = timeWindow;
    }

    //缓存在query到来之前tra的状态
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
    public void processElement1(TraPoint point, KeyedCoProcessFunction<Long, TraPoint, QueryInfo, QueryTraInfo>.Context ctx, Collector<QueryTraInfo> out) throws Exception {
        Trajectory tra = traState.value();

        //新增grid
        Tuple2<Integer, Integer> newGridID = GridTool.fromPoint(gridLen, gridRate, point);
        point.gridID = newGridID;
        if (!gridCntMap.contains(newGridID)) gridCntMap.put(newGridID, 0);
        gridCntMap.put(newGridID, gridCntMap.get(newGridID) + 1);

        //删去旧grid
        Set<TraPoint> oldPoints = tra.addPoint(point);
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

    @Override
    public void processElement2(QueryInfo info, KeyedCoProcessFunction<Long, TraPoint, QueryInfo, QueryTraInfo>.Context ctx, Collector<QueryTraInfo> out) throws Exception {
        Trajectory tra = traState.value();
        if (tra.id == -1) {
            System.out.println("invalid query");
            return;
        }
        tra = SerializationUtils.clone(tra);
        QueryTraInfo queryTraInfo = new QueryTraInfo(tra, info);
        //计算相关Grids
        queryTraInfo.relavantGridIDs = new HashSet<>();
        gridCntMap.keys().forEach(gridID -> queryTraInfo.relavantGridIDs.add(gridID));

        out.collect(queryTraInfo);
    }
}
