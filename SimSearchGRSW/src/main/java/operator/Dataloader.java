package operator;

import entity.TraPoint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class Dataloader implements FlatMapFunction<String, TraPoint> {

    public int dataset;
    public int dataSplitScale;

    public Dataloader(int dataset, int dataSplitScale) {
        this.dataset = dataset;
        this.dataSplitScale = dataSplitScale;
    }

    @Override
    public void flatMap(String line, Collector<TraPoint> out) throws Exception {
        if (dataset == 1) {
            tdriveLoader(line, out);
        } else if (dataset == 2) {
            brinkhoffLoader(line, out);
        } else {
            aisLoader(line, out);
        }
    }

    public void tdriveLoader(String line, Collector<TraPoint> out) throws Exception {
        String[] strs = line.split(",");
        long id = Long.parseLong(strs[0]);
        if (!dataScaleFilter(id)) return;
        double x = Double.parseDouble(strs[2]);
        if (x < 116.0) x = 116.0;
        else if (x > 116.8) x = 116.8;
        double y = Double.parseDouble(strs[3]);
        if (y < 39.5) y = 39.5;
        else if (y > 40.3) y = 40.3;
        out.collect(new TraPoint(x, y, dateToTimestamp(strs[1], "yyyy-MM-dd HH:mm:ss"), id));
    }

    public void brinkhoffLoader(String line, Collector<TraPoint> out) throws Exception {
        String[] strs = line.split(","); //id,time,x,y
        long id = Long.parseLong(strs[0]);
        if (!dataScaleFilter(id)) return;
        double x = Double.parseDouble(strs[2]);
        if (x < 281) x = 281;
        else if (x > 23854) x = 23854;
        double y = Double.parseDouble(strs[3]);
        if (y < 3935) y = 3935;
        else if (y > 26916) y = 26916;
        out.collect(new TraPoint(x, y, Long.parseLong(strs[1]), id));
    }

    public void aisLoader(String line, Collector<TraPoint> out) throws Exception {
        String[] strs = line.split(",");
        long id = Long.parseLong(strs[0]);
        if (!dataScaleFilter(id)) return;
        double x = Double.parseDouble(strs[2]);
        double y = Double.parseDouble(strs[3]);
        out.collect(new TraPoint(x, y, dateToTimestamp(strs[1], "yyyy-MM-dd'T'HH:mm:ss"), id));
    }

    public boolean dataScaleFilter(Long id) {
        long mod = id % 5;
        boolean filter = false;
        for (int i = 0; i <= dataSplitScale - 1; i++) {
            filter = filter || (mod == i);
        }
        return filter;
    }

    public long dateToTimestamp(String date, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return LocalDateTime.parse(date, formatter).toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
    }
}
