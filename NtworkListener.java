import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import java.util.Arrays;
import scala.Tuple2;


public class NtworkListener {

    private static final String HOST = "localhost";
    private static final int PORT = 9999;
    private static final long X = 10;
    private static final long Y = 30;
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\winutils\\bin");
//        set SPARK configuration
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("NtworkListener");
//        create SPARK Streaming Context
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(Y));
//        Receive RDD from Socket
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream(HOST, PORT);
//        map lines to the form: key=line,v1=(port+ip source), v2 = (port+ip target)
        JavaPairDStream<String, Tuple2> sourcesFull = lines.mapToPair(x-> {
            return new Tuple2<>(x,new Tuple2<>(x.split(",")[0]+','+x.split(",")[1], x.split(",")[0]+','+x.split(",")[2]));
        });
//        flatering of tuples to get flat port+ip
        JavaPairDStream<String,String> sources = sourcesFull.flatMapValues(x ->  Arrays.asList(x._1.toString(),x._2.toString()));
//        reassign key/value for calculation
        JavaPairDStream<String,Integer> sourcesC = sources.mapToPair(x -> new Tuple2(x._2, 1));
//        count port+ip and filter it
        JavaPairDStream<String,Integer> reduced = sourcesC.reduceByKey((a, b) -> a + b).filter(x->(x._2>X));
//		JavaPairDStream<Integer,String> swaped = reduced.mapToPair(x->x.swap());
//		JavaPairDStream<Integer,String> sorted = swaped.transformToPair(x->x.sortByKey(true));
//		//Cache it
//        JavaPairDStream<String,Integer> swapedback = sorted.mapToPair(x->x.swap());
//print the result
        reduced.cache();
        reduced.print();
        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate
    }
}
