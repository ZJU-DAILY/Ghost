package tool;

import entity.TraPoint;
import org.apache.flink.api.java.tuple.Tuple2;

public class GridTool {

    public static Tuple2<Integer, Integer> fromLocation(double gridLen, long rate, double lon, double lat) {
        int x = (int) (lon * rate) / (int) gridLen;
        int y = (int) (lat * rate) / (int) gridLen;
        return Tuple2.of(x, y);
    }

    public static Tuple2<Integer, Integer> fromPoint(double gridLen, long rate, TraPoint p) {
        return fromLocation(gridLen, rate, p.x, p.y);
    }
}
