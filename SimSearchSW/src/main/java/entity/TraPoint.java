package entity;

import java.io.Serializable;

public class TraPoint implements Serializable {
    public double x, y;
    public long t, id;

    public TraPoint(double x, double y, long t, long id) {
        this.x = x;
        this.y = y;
        this.t = t;
        this.id = id;
    }

    @Override
    public String toString() {
        return "(" + x + "," + y + ")";
    }
}
