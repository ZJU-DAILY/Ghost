package entity;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;

public class Trajectory implements Serializable {

    public long timeWindow; // >0
    public long id = -1;
    public LinkedList<TraPoint> points = new LinkedList<>();

    public Trajectory(long timeWindow) {
        this.timeWindow = timeWindow;
    }

    public void addPoint(TraPoint p) {
        if (points.isEmpty()) {
            points.addLast(p);
            id = p.id;
        } else {
            //insert
            ListIterator<TraPoint> pointListIterator = points.listIterator(points.size());
            int idxToInsert = points.size();
            while (pointListIterator.hasPrevious()) {
                TraPoint curP = pointListIterator.previous();
                if (p.t >= curP.t) {
                    break;
                }
                idxToInsert--;
            }
            points.add(idxToInsert, p);

            while (points.size() > 1 && points.getLast().t - points.getFirst().t > timeWindow) {
                points.removeFirst();
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("Tra[id=%d]:", id));
        Iterator<TraPoint> it = points.iterator();
        while (it.hasNext()) {
            TraPoint p = it.next();
            if (p != points.getFirst()) builder.append("->");
            builder.append(p);
        }
        return builder.toString();
    }
}
