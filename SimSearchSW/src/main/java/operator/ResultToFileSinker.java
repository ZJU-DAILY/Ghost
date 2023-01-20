package operator;

import entity.QueryPair;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.*;

public class ResultToFileSinker extends RichSinkFunction<QueryPair> {

    public String sinkDir;

    public ResultToFileSinker(String sinkDir) {
        this.sinkDir = sinkDir;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        File sinkDirFile = new File(sinkDir);
        deleteDirRecursively(sinkDirFile);
        if (!sinkDirFile.exists()) sinkDirFile.mkdirs();
    }

    @Override
    public void invoke(QueryPair queryPair, Context context) throws Exception {
        super.invoke(queryPair, context);
        System.out.println(queryPair.toString());

        //写入文件
        String filePath = sinkDir + queryPair.queryTra.id + ".txt";
        File file = new File(filePath);
        if (!file.exists()) file.createNewFile();
        FileWriter writer = new FileWriter(file);
        writer.write(queryPair.toString());
        writer.flush();
        writer.close();
    }

    @Override
    public void close() throws Exception {
        super.close();
        //聚合搜索结果
        File aggFile = new File(sinkDir + "0_aggregate.txt");
        aggFile.createNewFile();
        FileWriter aggWriter = new FileWriter(aggFile);

        int cnt = 0;
        int validCnt = 0;
        long startTimestamp = Long.MAX_VALUE;
        long endTimestamp = 0;
        int avgTime = 0;

        File sinkDirFile = new File(sinkDir);
        for (File sinkFile : sinkDirFile.listFiles()) {

            if (sinkFile.getName().contains("0_aggregate.txt")) continue;
            cnt++;

            FileInputStream sinkIS = new FileInputStream(sinkFile);
            BufferedReader reader = new BufferedReader(new InputStreamReader(sinkIS));
            String line = reader.readLine();
            if (line == null || line.equals("")) continue;
            String[] strs = line.split(",");
            if (strs.length <= 1) return;

            //queryId=%d,similar=%d(total=%d),start=%d,end=%d,time(ms)=%d
            startTimestamp = Math.min(startTimestamp, Long.parseLong( strs[2].split("=")[1]) );
            endTimestamp = Math.max(endTimestamp, Long.parseLong( strs[3].split("=")[1])) ;
            long singleTime = Long.parseLong( strs[4].split("=")[1] );
            if (singleTime >= 0) {
                validCnt++;
                avgTime += singleTime;
            }

            sinkIS.close();
            reader.close();
        }

        if (validCnt > 0) avgTime /= validCnt;

        aggWriter.write(String.format(
                "[AGGREGATE]total=%d(valid=%d),total_time(ms)=%d(start=%d,end=%d),avg_time(ms)=%d\n",
                cnt, validCnt, endTimestamp - startTimestamp, startTimestamp, endTimestamp, avgTime
        ));
        aggWriter.flush();
        aggWriter.close();

    }

    public static void deleteDirRecursively(File file) {
        if (!file.exists()) return;
        if (file.isDirectory()) {
            File[] childrenFile = file.listFiles();
            for (File childFile : childrenFile) {
                deleteDirRecursively(childFile);
            }
        } else {
            file.delete();
        }
    }
}
