package measure;

import entity.TraPoint;
import entity.Trajectory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

public class ERP implements Serializable,Measure {
    public PointDist pointDist;
    public TraPoint gap;

    public ERP(PointDist pointDist, TraPoint gap) {
        this.pointDist = pointDist;
        this.gap = gap;
    }

    @Override
    public double traDist(Trajectory t1, Trajectory t2) {
        LinkedList<TraPoint> ps1 = t1.points;
        LinkedList<TraPoint> ps2 = t2.points;
        int m = ps1.size(), n = ps2.size();

        Map<TraPoint, Double> mapGapDist = new HashMap<>();
        ps1.forEach(point -> {
            mapGapDist.put(point, pointDist.calc(point, gap));
        });
        ps2.forEach(point -> {
            mapGapDist.put(point, pointDist.calc(point, gap));
        });

        double[][] dp = new double[m + 1][n + 1];
        dp[0][0] = 0;
        double accu = 0;
        Iterator<TraPoint> it1 = ps1.iterator();
        for (int i = 1; i <= m; i++) {
            TraPoint p = it1.next();
            accu += mapGapDist.get(p);
            dp[i][0] = accu;
        }
        accu = 0;
        Iterator<TraPoint> it2 = ps2.iterator();
        for (int j = 1; j <= n; j++) {
            TraPoint p = it2.next();
            accu += mapGapDist.get(p);
            dp[0][j] = accu;
        }

        it1 = ps1.iterator();
        for (int i = 1; i <= m; i++) {
            TraPoint p1 = it1.next();
            it2 = ps2.iterator();
            for (int j = 1; j <= n; j++) {
                TraPoint p2 = it2.next();
                dp[i][j] = Math.min(dp[i - 1][j - 1] + pointDist.calc(p1, p2),
                        Math.min(dp[i - 1][j] + mapGapDist.get(p1), dp[i][j - 1] + mapGapDist.get(p2))
                );
            }
        }
        return dp[m][n];
    }
}
