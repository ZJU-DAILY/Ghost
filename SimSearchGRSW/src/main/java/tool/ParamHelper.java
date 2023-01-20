package tool;

import entity.TraPoint;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

public class ParamHelper {

    // properties file
    public static final String propertiesFilePath = "";

    private static ParameterTool paramTool;

    static {
        try {
            paramTool = ParameterTool.fromPropertiesFile(propertiesFilePath);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("failed to init parameter tool");
        }
    }

    public static void initFromArgs(String[] args) {
        if (args != null && args.length != 0) {
            ParameterTool argParameter = ParameterTool.fromArgs(args);
            paramTool = paramTool.mergeWith(argParameter);
        } else if (paramTool == null) {
            System.out.println("no valid parameters");
        }
        System.out.println("============Overall Params============");
        paramTool.toMap().forEach((k, v) -> System.out.println(k + ": " + v));
        System.out.println("======================================");
    }

    public static int getDataset() {
        return paramTool.getInt("dataset");
    }

    public static String getTDrivePath() {
        return paramTool.getRequired("tdrive_path");
    }

    public static int getTDriveSize() {
        return paramTool.getInt("tdrive_size");
    }

    public static String getBrinkhoffPath() {
        return paramTool.getRequired("brinkhoff_path");
    }

    public static int getBrinkhoffSize() {
        return paramTool.getInt("brinkhoff_size");
    }

    public static String getAISPath() {
        return paramTool.getRequired("ais_path");
    }

    public static int getAISSize() {
        return paramTool.getInt("ais_size");
    }

    public static String getSinkDir() {
        return paramTool.getRequired("sink_dir");
    }

    public static String getQueryPath() {
        return paramTool.getRequired("query_path");
    }

    public static String getSocketStreamHost() {
        return paramTool.getRequired("socket_stream_host");
    }

    public static int getSocketStreamPort() {
        return paramTool.getInt("socket_stream_port");
    }


    public static int getParallelism() {
        return paramTool.getInt("parallelism");
    }

    public static long getDelayReduceTime() {
        return paramTool.getLong("delay_reduce");
    }


    public static int getDistMeasure() {
        return paramTool.getInt("dist_measure");
    }

    public static long getTimeWindowSize() {
        return paramTool.getLong("time_window");
    }

    public static double getGridLength() { return paramTool.getDouble("grid_len"); }

    public static int getDataSplitScale() {
        return paramTool.getInt("data_scale");
    }

    public static double getLCSSThreshold() {
        return paramTool.getDouble("lcss_threshold");
    }

    public static int getLCSSDelta() {
        return paramTool.getInt("lcss_delta");
    }

    public static double getEDRThreshold() {
        return paramTool.getDouble("edr_threshold");
    }

    public static TraPoint getERPGap() {
        double x = paramTool.getDouble("erp_gap_x");
        double y = paramTool.getDouble("erp_gap_y");
        return new TraPoint(x, y, -1, -1);
    }
}
