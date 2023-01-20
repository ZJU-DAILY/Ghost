package operator;

import com.github.davidmoten.rtreemulti.Entry;
import com.github.davidmoten.rtreemulti.Node;
import com.github.davidmoten.rtreemulti.NonLeaf;
import com.github.davidmoten.rtreemulti.RTree;
import com.github.davidmoten.rtreemulti.geometry.Point;
import com.github.davidmoten.rtreemulti.geometry.Rectangle;
import entity.QueryPair;
import entity.TraPoint;
import entity.Trajectory;
import measure.Measure;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

public class RTreePruner extends KeyedProcessFunction<Long, QueryPair, QueryPair> {
    public long timeWindow;
    public Measure measure;

    public RTreePruner(long timeWindow, Measure measure) {
        this.timeWindow = timeWindow;
        this.measure = measure;
    }

    private ValueState<Boolean> queryTraLoadState;
    private ValueState<RTree<TraPoint, Point>> rtreeState;
    public ValueState<Rectangle> mbrState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        queryTraLoadState = getRuntimeContext().getState(
                new ValueStateDescriptor<Boolean>("queryTraLoadState", Boolean.class, false)
        );
        rtreeState = getRuntimeContext().getState(
                new ValueStateDescriptor<RTree<TraPoint, Point>>("rtreeState",
                        TypeInformation.of(new TypeHint<RTree<TraPoint, Point>>() {
                        }),
                        RTree.create())
        );
        mbrState = getRuntimeContext().getState(
                new ValueStateDescriptor<Rectangle>("mbrState", Rectangle.class, null)
        );
    }

    @Override
    public void processElement(QueryPair pair, KeyedProcessFunction<Long, QueryPair, QueryPair>.Context ctx, Collector<QueryPair> out) throws Exception {
        RTree<TraPoint, Point> rtree = rtreeState.value();
        Boolean loaded = queryTraLoadState.value();
        //加载queryTra到rtree
        Trajectory queryTra = pair.queryTra;
        if (!loaded) {
            for (TraPoint traPoint : queryTra.points) {
                rtree = rtree.add(traPoint, traPoint.getGeometryPoint());
            }
            rtreeState.update(rtree);
            queryTraLoadState.update(true);
        }
        //加载anotherTra到rtree
        Trajectory anotherTra = pair.anotherTra;
        if (anotherTra.id != queryTra.id) {
            for (TraPoint traPoint : anotherTra.points) {
                rtree = rtree.add(traPoint, traPoint.getGeometryPoint());
            }
            rtreeState.update(rtree);
        }

        //找到覆盖的mbr
        Rectangle mbr = rtree.root().isPresent() ? null : findContainedMbr(rtree, queryTra, pair.threshold, rtree.root().get());
        if (mbr == null) {
            out.collect(pair);
        } else {
            for (TraPoint traPoint : anotherTra.points) {
                if (mbr.contains(traPoint.x, traPoint.y)) {
                    out.collect(pair);
                    return;
                }
            }
        }
    }

    public Rectangle findContainedMbr(RTree<TraPoint, Point> rtree, Trajectory tra, Double threshold, Node<TraPoint, Point> oriNode) {
        if (oriNode.isLeaf()) return null;
        Rectangle mbr = oriNode.geometry().mbr();
        if (!isTrajectoryCovered(rtree, mbr, tra) || !isSimilarTraCovered(mbr, tra, threshold)) return null;
        NonLeaf<TraPoint, Point> node = (NonLeaf<TraPoint, Point>) oriNode;
        for (Node<TraPoint, Point> childNode : node.children()) {
            Rectangle childMbr = findContainedMbr(rtree, tra, threshold, childNode);
            if (childMbr != null && childMbr.volume() < mbr.volume()) {
                mbr = childMbr;
            }
        }
        return mbr;
    }

    public boolean isTrajectoryCovered(RTree<TraPoint, Point> rtree, Rectangle mbr, Trajectory tra) {
        Set<TraPoint> traPoints = new HashSet<>(tra.points);
        int n = traPoints.size();
        int cnt = 0;
        for (Entry<TraPoint, Point> entry : rtree.search(mbr)) {
            TraPoint curTraPoint = entry.value();
            if (traPoints.contains(curTraPoint)) cnt++;
        }
        return cnt == n;
    }

    public boolean isSimilarTraCovered(Rectangle mbr, Trajectory tra, double threshold) {
        TraPoint minl = new TraPoint(mbr.min(0), mbr.min(1), -1, -1);
        TraPoint maxr = new TraPoint(mbr.max(0), mbr.max(1), -1, -1);

        Trajectory another = new Trajectory(timeWindow);
        for (TraPoint traPoint : tra.points) {
            //找到离边界最近的点
            double[] lens = new double[4];// 左 上 右 下
            lens[0] = traPoint.x - minl.x;
            lens[1] = maxr.y - traPoint.y;
            lens[2] = maxr.x - traPoint.x;
            lens[3] = traPoint.y - minl.y;

            int minIdx = -1;
            double minval = Double.MAX_VALUE;
            for (int i = 0; i < 4; i++) {
                if (lens[i] < 0) return false;
                if (lens[i] < minval) {
                    minval = lens[i];
                    minIdx = i;
                }
            }
            double x = traPoint.x, y = traPoint.y;
            switch (minIdx) {
                case 0:
                    x = minl.x;
                    break;
                case 1:
                    y = maxr.y;
                    break;
                case 2:
                    x = maxr.x;
                    break;
                case 3:
                    y = minl.y;
                    break;
            }
            another.addPoint(new TraPoint(x, y, traPoint.t, -1));
        }
        return measure.traDist(tra, another) >= threshold;
    }
}
