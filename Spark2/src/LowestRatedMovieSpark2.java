
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.AnalysisException;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class LowestRatedMovieSpark2 {

    private static void runInferSchemaExample(SparkSession spark) {
        // Create an RDD of MovieRating objects from a text file
        JavaRDD<MovieRating> peopleRDD = spark.read()
                .textFile("ml-100k/u.data")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split("\\t");
                    MovieRating movieRating = new MovieRating();
                    movieRating.setMovieId(Integer.valueOf(parts[1]));
                    movieRating.setRating(Integer.valueOf(parts[2]));
                    return movieRating;
                });

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> ds_movieRating = spark.createDataFrame(peopleRDD, MovieRating.class);
        // Register the DataFrame as a temporary view
        ds_movieRating.createOrReplaceTempView("movie_rating");

        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> reallyBadMoviesDF = spark.sql("SELECT movieID, COUNT(movieID) as ratingCount " +
                "FROM movie_rating GROUP BY movieID HAVING COUNT(movieID) > 10");
        System.out.println("Movies rated more than 10 times");
        reallyBadMoviesDF.orderBy("ratingCount").show(100);

        //Compute average rating for each movieID
        Dataset<Row> ds_averageRatings = ds_movieRating.groupBy("movieID").avg("rating");
        System.out.println("ds_averageRatings");
        ds_averageRatings.show(100);

        //Compute count of ratings for each movieID
        Dataset<Row> ds_counts = ds_movieRating.groupBy("movieID").count();
        ds_counts = ds_counts.sort(col("count").desc());
        System.out.println("ds_counts descending");
        ds_counts.show(100);


        //Join the two together (We now have movieID, avg(rating), and count columns)
        Dataset<Row> ds_averagesAndCounts = ds_counts.join(ds_averageRatings, "movieID");
        System.out.println("ds_averagesAndCounts");
        ds_averagesAndCounts.show(100);

        //Filter movies rated 10 or fewer times
        Dataset<Row> ds_popularAveragesAndCounts = ds_averagesAndCounts.filter("count > 10");
        System.out.println("ds_popularAveragesAndCounts");
        ds_popularAveragesAndCounts.show(100);

        //Pull the top 10 results
        for(String s : ds_popularAveragesAndCounts.columns())
            System.out.print(s + " | ");
        System.out.println("");
        List<Row> top10List = ds_popularAveragesAndCounts.orderBy("avg(rating)").takeAsList(10);
        for(Row r : top10List)
            System.out.println(r);
    }

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        runInferSchemaExample(spark);

        spark.stop();
    }

}
