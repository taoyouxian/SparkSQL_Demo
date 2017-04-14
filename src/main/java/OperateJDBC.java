import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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

        runJdbcDatasetExample(spark);
        spark.stop();
    }

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
        try {
            Dataset<Row> jdbcDF = spark.read().jdbc("jdbc:postgresql://localhost:5432/postgres", "weather", connectionProperties);
            RelationalGroupedDataset rgDataset = jdbcDF.select("location", "month", "temperature").filter("location = 'BRBRGTWN'").orderBy("month").groupBy("location", "month");
            Dataset<Row> jdbcDF2 = rgDataset.avg("temperature");
            //显示rgDataset数据内容
            jdbcDF2.show();
        } catch (Exception e) {
            System.out.print("错误信息: " + e.getMessage());
        }
    }

}
