package measure;

import entity.TraPoint;
import entity.Trajectory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;

public class EDR implements Serializable, Measure {
    public PointDist pointDist;
    public double threshold;

    public EDR(PointDist pointDist, double threshold) {
        this.pointDist = pointDist;
        this.threshold = threshold;
    }

    @Override
    public double traDist(Trajectory t1, Trajectory t2) {
        LinkedList<TraPoint> ps1 = t1.points;
        LinkedList<TraPoint> ps2 = t2.points;
        int m = ps1.size(), n = ps2.size();
        int[][] dp = new int[m + 1][n + 1];
        dp[0][0] = 0;
        for (int i = 1; i <= m; i++) dp[i][0] = i;
        for (int j = 1; j <= n; j++) dp[0][j] = j;

        Iterator<TraPoint> it1 = ps1.iterator();
        for (int i = 1; i <= m; i++) {
            TraPoint p1 = it1.next();
            Iterator<TraPoint> it2 = ps2.iterator();
            for (int j = 1; j <= n; j++) {
                TraPoint p2 = it2.next();
                int subcost = pointDist.calc(p1, p2) <= threshold ? 0 : 1;
                dp[i][j] = Math.min(dp[i - 1][j - 1] + subcost, Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1));
            }
        }
        return dp[m][n];
    }
}
