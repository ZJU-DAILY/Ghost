package measure;

import entity.TraPoint;

import java.io.Serializable;

public class PointDistOnGPS implements PointDist, Serializable {
    @Override
    public double calc(TraPoint p1, TraPoint p2) {
        double lon1 = p1.x, lon2 = p2.x;
        double lat1 = p1.y, lat2 = p2.y;

        double EARTH_RADIUS = 6378137;

        lat1 = lat1 * Math.PI / 180.0;
        lat2 = lat2 * Math.PI / 180.0;
        double sa2 = Math.sin((lat1 - lat2) / 2.0);
        double sb2 = Math.sin(((lon1 - lon2) * Math.PI / 180.0) / 2.0);
        return 2 * EARTH_RADIUS * Math.asin(Math.sqrt(sa2 * sa2 + Math.cos(lat1) * Math.cos(lat2) * sb2 * sb2));
    }
}
