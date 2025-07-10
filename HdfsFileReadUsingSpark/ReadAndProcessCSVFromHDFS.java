package HdfsFileReadUsingSpark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class ReadAndProcessCSVFromHDFS {
    public static void main(String[] args) {
        // Initialize a Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Read and Process CSV from HDFS")
                .master("local[*]")  // Use this for local testing. Adjust when running on a cluster.
                .getOrCreate();

        // HDFS path to the CSV file (replace with your actual HDFS path)
        String hdfsCsvFilePath = "hdfs://192.168.0.122:19000/mhub/data/sugan";

        // Read the CSV file into a DataFrame
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")  // Assumes the first row is the header
                .option("inferSchema", "true")  // Automatically infers column types
                .load(hdfsCsvFilePath);

        // Show the first 5 rows for verification
        System.out.println("Original Data:");
        df.show(5);

        // Filtering: Keep only records where age is greater than 30
        Dataset<Row> filteredDF = df.filter(df.col("trip_distance").gt(5));

        // Show filtered data
        System.out.println("Filtered Data (trip_distance > 30):");
        filteredDF.show(5);

        // Aggregation: Calculate average fare for each payment_type group
        Dataset<Row> aggregatedDF = filteredDF.groupBy("payment_type")
                .agg(functions.avg("fare_amount").alias("average_fare"));

        // Show aggregated results
        System.out.println("Average fare_amount by payment_type Group:");
        aggregatedDF.show();

        // Count the number of records in the filtered DataFrame
        long filteredCount = filteredDF.count();
        System.out.println("Number of records with payment_type > 30: " + filteredCount);

        // Perform a calculation: Add a new column (e.g., fare tip after 10% raise)
        Dataset<Row> updatedDF = filteredDF.withColumn("fare_amount_with_tip", df.col("fare_amount").multiply(1.10));

        Dataset<Row> selectedColDF = updatedDF.select("payment_type","fare_amount","fare_amount_with_tip");
                //.groupBy("payment_type").agg(functions.avg("fare_amount").alias("average_fare"));

        // Show updated DataFrame with salary after a 10% raise
        System.out.println("Updated Data with fare_amount After 10% tip:");
        selectedColDF.show();

        // Stop the Spark session
        spark.stop();
    }
}