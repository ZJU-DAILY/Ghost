package job;

import entity.QueryInfo;
import entity.QueryPair;
import entity.QueryTraInfo;
import entity.TraPoint;
import measure.*;
import operator.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import tool.ParamHelper;

public class SimSearch {
    public static int dataset;

    public static String dataPath;
    public static int dataSize;

    public static String sinkDir;
    public static String queryPath;

    public static String socketStreamHost;
    public static int socketStreamPort;

    public static int parallelism;
    public static long delayReduceTime;

    public static double lcssThr;
    public static int lcssDelta;
    public static double edrThr;
    public static TraPoint erpGap;

    public static PointDist pointDist;
    public static Measure distMeasure;
    public static long timeWindowSize;
    public static int dataSplitScale;


    public static void main(String[] args) throws Exception {
        //init params
        ParamHelper.initFromArgs(args);
        dataset = ParamHelper.getDataset();
        if (dataset == 1) {
            dataPath = ParamHelper.getTDrivePath();
            dataSize = ParamHelper.getTDriveSize();
            pointDist = new PointDistOnGPS();
        } else if (dataset == 2) {
            dataPath = ParamHelper.getBrinkhoffPath();
            dataSize = ParamHelper.getBrinkhoffSize();
            pointDist = new PointDistOnFlat();
        } else {
            dataPath = ParamHelper.getAISPath();
            dataSize = ParamHelper.getAISSize();
            pointDist = new PointDistOnGPS();
        }
        sinkDir = ParamHelper.getSinkDir();
        queryPath = ParamHelper.getQueryPath();
        socketStreamHost = ParamHelper.getSocketStreamHost();
        socketStreamPort = ParamHelper.getSocketStreamPort();
        parallelism = ParamHelper.getParallelism();
        delayReduceTime = ParamHelper.getDelayReduceTime();
        lcssThr = ParamHelper.getLCSSThreshold();
        lcssDelta = ParamHelper.getLCSSDelta();
        edrThr = ParamHelper.getEDRThreshold();
        erpGap = ParamHelper.getERPGap();
        int dist_measure_op = ParamHelper.getDistMeasure();
        if (dist_measure_op == 1) {
            distMeasure = new DTW(pointDist);
        } else if (dist_measure_op == 2) {
            distMeasure = new LCSS(pointDist, lcssThr, lcssDelta);
        } else if (dist_measure_op == 3) {
            distMeasure = new EDR(pointDist, edrThr);
        } else {
            distMeasure = new ERP(pointDist, erpGap);
        }
        timeWindowSize = ParamHelper.getTimeWindowSize();
        dataSplitScale = ParamHelper.getDataSplitScale();

        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        new SimSearch().apply(env);

        //run
        env.execute("Similarity Search without Index");
    }

    public void apply(StreamExecutionEnvironment env) {
        //从socket读取指令
        DataStreamSource<String> commandStream = env
                .socketTextStream(socketStreamHost, socketStreamPort, "\n");
        //从hdfs读取的query字符串
        SingleOutputStreamOperator<QueryInfo> queryInfoStream = env
                .readTextFile(queryPath).keyBy(queryline -> 1).connect(commandStream.keyBy(command -> 1))
                .process(new CoQueryInfoLoader(queryPath));

//        //从socket读取query信息
//        SingleOutputStreamOperator<QueryInfo> queryInfoStream = env
//                .socketTextStream(socketStreamHost, socketStreamPort, "\n")
//                .flatMap(new QueryInfoLoader(queryDir));

        //按query轨迹的id分区
        KeyedStream<QueryInfo, Long> keyedQueryInfoStream = queryInfoStream.keyBy(info -> info.queryTraId);

        //从数据源读取point流
        SingleOutputStreamOperator<TraPoint> pointStream = env
                .readTextFile(dataPath)
                .setParallelism(336)
                .keyBy(line -> Long.parseLong(line.split(",")[0]))
                .flatMap(new Dataloader(dataset, dataSplitScale))
                .setParallelism(336);
        //按点的id分区
        KeyedStream<TraPoint, Long> keyedPointStream = pointStream.keyBy(point -> point.id);

        //通过queryInfo生成queryTra
        SingleOutputStreamOperator<QueryTraInfo> queryTraInfoStream = pointStream.connect(queryInfoStream)
                .keyBy(point -> point.id, info -> info.queryTraId)
                .process(new QueryTraInfoGenerator(timeWindowSize))
                .setParallelism(parallelism);

        //广播queryTraInfo到每一条point流，生成queryPair计算对象
        SingleOutputStreamOperator<QueryTraInfo> broadcastQueryTraInfoStream = queryTraInfoStream
                .keyBy(queryTraInfo -> queryTraInfo.info.queryTraId)
                .flatMap(new QueryTraInfoBroadcaster(dataSize))
                .setParallelism(parallelism);

        SingleOutputStreamOperator<QueryPair> queryPairStream = broadcastQueryTraInfoStream.connect(pointStream)
                .keyBy(queryTraInfo -> queryTraInfo.anotherTraId, point -> point.id)
                .process(new QueryPairGenerator(timeWindowSize))
                .setParallelism(parallelism);

        //打时间戳
        queryPairStream = queryPairStream
                .map(pair -> {
                    pair.startTimestamp = System.currentTimeMillis();
                    return pair;
                })
                .setParallelism(parallelism);

        SingleOutputStreamOperator<QueryPair> calculatedQueryPairStream = queryPairStream
                //根据<t1.id,t2.id>分区
                .keyBy(new KeySelector<QueryPair, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(QueryPair pair) throws Exception {
                        return Tuple2.of(pair.queryTra.id, pair.anotherTra.id);
                    }
                })
                .process(new SimilarCalculator(distMeasure))
                .setParallelism(parallelism);

        //打时间戳
        calculatedQueryPairStream = calculatedQueryPairStream
                .map(pair -> {
                    pair.endTimestamp = System.currentTimeMillis();
                    return pair;
                })
                .setParallelism(parallelism);

        //统计时间戳
        final OutputTag<QueryPair> lateReduceQueryPair = new OutputTag<QueryPair>("lateReduceQueryPair") {
        };

        SingleOutputStreamOperator<QueryPair> reduceQueryPairStream = calculatedQueryPairStream
                .keyBy(pair -> pair.queryTra.id)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(delayReduceTime)))
                .sideOutputLateData(lateReduceQueryPair)
                .reduce(new ResultReducer())
                .setParallelism(parallelism);

        SingleOutputStreamOperator<QueryPair> resultQueryPairStream = reduceQueryPairStream.connect(reduceQueryPairStream.getSideOutput(lateReduceQueryPair))
                .keyBy(pair -> pair.queryTra.id, pair -> pair.queryTra.id)
                .map(new CoMapFunction<QueryPair, QueryPair, QueryPair>() {
                    @Override
                    public QueryPair map1(QueryPair value) throws Exception {
                        return value;
                    }

                    @Override
                    public QueryPair map2(QueryPair value) throws Exception {
                        return value;
                    }
                })
                .keyBy(pair -> pair.queryTra.id)
                .reduce(new ResultReducer())
                .setParallelism(parallelism);

        //写入文件
        resultQueryPairStream.addSink(new ResultToFileSinker(sinkDir));
    }
}
