package operator;

import entity.QueryInfo;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.regex.Pattern;

public class CoQueryInfoLoader extends KeyedCoProcessFunction<Integer, String, String, QueryInfo> {

    public Pattern querylinePattern = Pattern.compile("[0-9]+,[.0-9]+");
    public String queryPath;

    public CoQueryInfoLoader(String queryPath) {
        this.queryPath = queryPath;
    }

    private ListState<QueryInfo> queryInfoList;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        queryInfoList = getRuntimeContext().getListState(new ListStateDescriptor<QueryInfo>("queryInfoList", QueryInfo.class));
    }

    @Override
    public void processElement1(String queryline, KeyedCoProcessFunction<Integer, String, String, QueryInfo>.Context ctx, Collector<QueryInfo> out) throws Exception {
        if (queryline != null && !queryline.equals("")) {
            String[] strs = queryline.split(",");
            queryInfoList.add(new QueryInfo(Long.parseLong(strs[0]), Double.parseDouble(strs[1])));
        }
    }

    @Override
    public void processElement2(String command, KeyedCoProcessFunction<Integer, String, String, QueryInfo>.Context ctx, Collector<QueryInfo> out) throws Exception {
        if (querylinePattern.matcher(command).matches()) {
            String[] strs = command.split(",");
            out.collect(new QueryInfo(Long.parseLong(strs[0]), Double.parseDouble(strs[1])));
        } else if (command.equals("file")) {
            queryInfoList.get().forEach(out::collect);
            queryInfoList.clear();
        }
    }
}
