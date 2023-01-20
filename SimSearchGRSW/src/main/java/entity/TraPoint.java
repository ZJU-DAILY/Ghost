package entity;

import com.github.davidmoten.rtreemulti.geometry.Point;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Objects;

public class TraPoint implements Serializable {
    public double x, y;
    public long t, id;
    public Tuple2<Integer, Integer> gridID;

    public TraPoint() {
    }

    public TraPoint(double x, double y, long t, long id) {
        this.x = x;
        this.y = y;
        this.t = t;
        this.id = id;
    }

    public Point getGeometryPoint() {
        return Point.create(x, y);
    }

    @Override
    public String toString() {
        return "(" + x + "," + y + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TraPoint)) return false;
        TraPoint traPoint = (TraPoint) o;
        return Double.compare(traPoint.x, x) == 0 && Double.compare(traPoint.y, y) == 0 && t == traPoint.t;
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y, t);
    }
}
