import edu.ruc.entity.Weather;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by Tao on 2017/4/13.
 */
public class OperateCsv {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL Csv")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        SparkConf conf = new SparkConf().setAppName("CSVDemo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> data = sc.textFile("/home/data/weather.csv");
        JavaRDD<Weather> wRDD = data.map(new Function<String, Weather>() {
            public Weather call(String line) throws Exception {
                String[] fields = line.split(",");
                Weather w = new Weather(fields[0], Integer.valueOf(fields[1], Integer.valueOf(fields[2], Integer.valueOf(fields[3]],
                Float.valueOf(fields[4]);
                return w;
            }
        });
        Dataset<Row> wDF = spark.createDataFrame(wRDD, Weather.class);
        wDF.createOrReplaceTempView("weather");
        Dataset<Row> weatherDF = spark.sql("SELECT location,month ,avg(temperature) FROM weather GROUP BY location, month ORDER BY month");
        weatherDF.show();
    }
}
