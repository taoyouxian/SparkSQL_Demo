import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Properties;

/**
 * Created by Tao on 2017/4/14.
 */
public class OperateJDBC {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL data sources example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

//        runJdbcDatasetExample(spark);
//        runCsvDatasetByparquet(spark);
        runCsvDatasetExample(spark);
        spark.stop();
    }

    /**
     * @param spark
     * @description "SELECT location,month ,avg(temperature) FROM weather where location = 'BRBRGTWN' GROUP BY location, month ORDER BY month"
     */
    private static void runCsvDatasetExample(SparkSession spark) {
        Dataset<Row> wDF = spark.read().option("header", "true").csv("E://weather.csv");

        long startTime = System.currentTimeMillis();

        try {
            RelationalGroupedDataset rgDataset = wDF.select("location", "month", "temperature").filter("location = 'BRBRGTWN'").orderBy("month").groupBy("location", "month");

//            Dataset<Row> jdbcDF2 = rgDataset.avg("temperature");
            long endTime = System.currentTimeMillis();
            System.out.println("程序运行时间： " + (endTime - startTime) + "ms");

            System.out.println("结果集： " + rgDataset.count());
//            jdbcDF2.show();
//            List<Row> weatherList = weatherDF.collectAsList();
//            System.out.println("程序运行时间： " + (endTime - startTime) + "ms");
//            System.out.println("查询列表: " + weatherList.size());
//            for (Row r : weatherList) {
//                System.out.println(r.toString());
//            }
        } catch (Exception e) {
            System.out.print("错误信息: " + e.getMessage());
        }
    }

    /**
     * @param spark
     * @description "SELECT location,month ,avg(temperature) FROM weather where location = 'BRBRGTWN' GROUP BY location, month ORDER BY month"
     */
    private static void runCsvDatasetByparquet(SparkSession spark) {
        Dataset<Row> wDF = spark.read().option("header", "true").csv("E://weather.csv");
        Path path = new Path("weather.parquet");
        Configuration conf = new Configuration();
        try {
            FileSystem fs = path.getFileSystem(conf);
            if (fs.exists(path)) {
                fs.delete(path, true);
            }
        } catch (Exception er) {
            System.out.println("Error Info: " + er.getMessage());
        }
        wDF.write().parquet("weather.parquet");

        long startTime = System.currentTimeMillis();

        Dataset<Row> parquetDF = spark.read().parquet("weather.parquet");
        parquetDF.createOrReplaceTempView("parquetCsv");
        try {
            Dataset<Row> weatherDF = spark.sql("SELECT location, month, avg(temperature) FROM parquetCsv where location = 'BRBRGTWN' GROUP BY location, month ORDER BY month");

            long endTime = System.currentTimeMillis();

            List<Row> weatherList = weatherDF.collectAsList();
            System.out.println("程序运行时间： " + (endTime - startTime) + "ms");
            System.out.println("查询列表: " + weatherList.size());
            for (Row r : weatherList) {
                System.out.println(r.toString());
            }
        } catch (Exception e) {
            System.out.print("错误信息: " + e.getMessage());
        }
    }


    /**
     * @param spark
     * @description "SELECT location,month ,avg(temperature) FROM weather where location = 'BRBRGTWN' GROUP BY location, month ORDER BY month"
     */
    private static void runJdbcDatasetExample(SparkSession spark) {
        //new一个属性
        System.out.println("确保数据库已经开启，并创建了weather表和插入了数据");
        Properties connectionProperties = new Properties();

        //增加数据库的用户名(user)密码(password),指定postgresql驱动(driver)
        System.out.println("增加数据库的用户名(user)密码(password),指定postgresql驱动(driver)");
        connectionProperties.put("user", "postgres");
        connectionProperties.put("password", "zenvisage");
        connectionProperties.put("driver", "org.postgresql.Driver");
        //SparkJdbc读取Postgresql的products表内容
        System.out.println("SparkJdbc读取Postgresql的weather表内容");

        long startTime = System.currentTimeMillis();
        try {
            Dataset<Row> jdbcDF = spark.read().jdbc("jdbc:postgresql://localhost:5432/postgres", "weather", connectionProperties);
            RelationalGroupedDataset rgDataset = jdbcDF.select("location", "month", "temperature").filter("location = 'BRBRGTWN'").orderBy("month").groupBy("location", "month");

            Dataset<Row> jdbcDF2 = rgDataset.avg("temperature");
            long endTime = System.currentTimeMillis();
            System.out.println("程序运行时间： " + (endTime - startTime) + "ms");
            //显示rgDataset数据内容
            jdbcDF2.show();
        } catch (Exception e) {
            System.out.print("错误信息: " + e.getMessage());
        }
    }

}
