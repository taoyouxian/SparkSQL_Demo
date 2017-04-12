import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Created by Tao on 2017/4/12.
 */
public class Operate {

    public static void main( String[] args ){
        String readme="/home/data/words";
        SparkConf conf=new SparkConf().setAppName("tiger's first spark app");
        JavaSparkContext sc =new JavaSparkContext(conf);
        JavaRDD<String> logData=sc.textFile(readme).cache();
        long num=logData.filter(new Function<String, Boolean>(){
            public Boolean call(String s){
                return s.contains("spark");
            }
        }).count();

        System.out.println("the count of word a is "+num);
    }
}
