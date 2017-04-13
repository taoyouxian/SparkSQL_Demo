import edu.ruc.entity.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

/**
 * Created by Tao on 2017/4/12.
 */
public class OperateSQL {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Spark SQL Example_group by").config("spark.some.config.option", "some-value").getOrCreate();
//        Dataset<Row> df = spark.read().json("/home/data/people.json");
//        df.createOrReplaceTempView("people");
//        Dataset<Row> sqlDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
//        sqlDF.show();
        // Create an RDD of Person objects from a text file
        JavaRDD<Person> peopleRDD = spark.read().textFile("/home/data/people.txt").javaRDD().map(new Function<String, Person>() {
//            @Override
            public Person call(String line) throws Exception {
                String[] parts = line.split(",");
                Person person = new Person();
                person.setName(parts[0]);
                person.setAge(Integer.parseInt(parts[1].trim()));
                return person;
            }
        });
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
        peopleDF.createOrReplaceTempView("people");

        Dataset<Row> teenagersDF = spark.sql("SELECT name, count(*) FROM people WHERE age BETWEEN 13 AND 19 group by name");

        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(new MapFunction<Row, String>() {
//            @Override
            public String call(Row row) throws Exception {
                return "Name: " + row.getString(0);
            }
        }, stringEncoder);
        teenagerNamesByIndexDF.show();

    }
}
