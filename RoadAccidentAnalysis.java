import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RoadAccidentAnalysis {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Road Accident CSV Analysis")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("hdfs://192.168.0.128:19000/project3/sugan/US_Accidents_2016-23.csv");

        df.printSchema();
        df.show(5, false);
        df.createOrReplaceTempView("accidents");

        // highest accident city
        spark.sql("SELECT city,COUNT(*) AS total_accidents\n" +
                "FROM accidents\n" +
                "GROUP by city\n" +
                "ORDER by total_accidents desc").show();

        // Hour of the day when accidents happen most
        spark.sql("SELECT HOUR(Start_Time) AS accident_hour,\n" +
                "COUNT(*) AS accident_count\n" +
                "FROM accidents\n" +
                "GROUP BY HOUR(Start_Time)\n" +
                "ORDER BY accident_count DESC").show();

        // Accidents on a specific date, grouped by cities
        spark.sql("SELECT City, DATE(Start_Time) AS Specific_Date,\n" +
                "COUNT(*) AS Accident_Count\n" +
                "FROM accidents\n" +
                "WHERE DATE(Start_Time) = '2022-12-23'\n" +
                "GROUP BY City, DATE(Start_Time)\n" +
                "ORDER BY Accident_Count desc").show();


        // Accidents count during day/night
        spark.sql("SELECT Sunrise_Sunset AS Day_Night\n," +
                "COUNT(*) AS Accidents_Count\n" +
                "FROM accidents\n" +
                "GROUP BY Sunrise_Sunset").show();


        spark.stop();
    }
}
