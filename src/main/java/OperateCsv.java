import edu.ruc.entity.Weather;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

/**
 * Created by Tao on 2017/4/13.
 */
public class OperateCsv {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Java Spark SQL Csv").config("spark.some.config.option", "some-value").getOrCreate();
        JavaRDD<Weather> wRDD = spark.read().option("header","true").textFile("/home/data/weather.csv").javaRDD().map(new Function<String, Weather>() {
            public Weather call(String line) throws Exception {
                String[] fields = line.split(",");
                Weather w = new Weather(fields[0], Integer.valueOf(fields[1]), Integer.valueOf(fields[2]), Integer.valueOf(fields[3]), Float.valueOf(fields[4]));
                return w;
            }
        });
        Dataset<Row> wDF = spark.createDataFrame(wRDD, Weather.class);
        wDF.createOrReplaceTempView("weather");
        Dataset<Row> weatherDF = spark.sql("SELECT location,month ,avg(temperature) FROM weather GROUP BY location, month ORDER BY month");
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> weatherByIndexDF = weatherDF.map(new MapFunction<Row, String>() {
            public String call(Row row) throws Exception {
                return "Name: " + row.getString(0);
            }
        }, stringEncoder);
        weatherByIndexDF.show();
    }
}
