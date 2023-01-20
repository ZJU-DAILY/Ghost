package measure;

import entity.TraPoint;

import java.io.Serializable;

public class PointDistOnFlat implements PointDist, Serializable {
    @Override
    public double calc(TraPoint p1, TraPoint p2) {
        double dx = p1.x - p2.x;
        double dy = p1.y - p2.y;
        return Math.sqrt(dx * dx + dy * dy);
    }
}
