package operator;

import entity.QueryInfo;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Pattern;

public class QueryInfoLoader implements FlatMapFunction<String, QueryInfo> {

    public String queryPath;

    public QueryInfoLoader(String queryPath) {
        this.queryPath = queryPath;
    }

    public static Pattern pattern = Pattern.compile("[0-9]+,[.0-9]+");

    @Override
    public void flatMap(String s, Collector<QueryInfo> collector) throws Exception {
        if (pattern.matcher(s).matches()) {
            String[] strs = s.split(",");
            collector.collect(new QueryInfo(Long.parseLong(strs[0]),
                    Double.parseDouble(strs[1])));
        } else if (s.contains(".query")) {
            // connect to file
            Files.lines(Paths.get(queryPath + s)).forEach(line -> {
                if (line != null && !line.equals("")) {
                    String[] strs = line.split(",");
                    collector.collect(new QueryInfo(Long.parseLong(strs[0]),
                            Double.parseDouble(strs[1])));
                }
            });
        }
    }
}
