import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

import scala.Function1;
import scala.collection.mutable.WrappedArray;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;


/**
 *  https://phoenixnap.com/kb/rdd-vs-dataframe-vs-dataset
 *
 *  RDD: Resilient Distributed Dataset
 *       Abstracted (mostly key and value) representation of data
 *       - that can handle failures in a resilient manner.
 *       - Distributed evenly across the cluster.
 *       - Looks & feels like a SQL Data Set.
 *
 *  Spark Context: what makes Spark resilient and distributed,
 *       - is the environment where the driver programs runs with.
 *       - creates the RDDs for driver program
 *
 */
public class MovieRecom2 {
    static Map<Integer, String> movieNames = null;
    static {
        movieNames = loadMovieNames();
    }

    private static Map<Integer, String> loadMovieNames() {
        HashMap<Integer, String> movieNames = new HashMap<>();
        int counter = 0;
        try (Scanner scanner = new Scanner(new File("ml-100k/u.item"), StandardCharsets.UTF_8.name())) {
            while (scanner.hasNext()) {
                String[] tokens = scanner.nextLine().split("\\|");
                Integer mID = Integer.valueOf(tokens[0]);
                movieNames.put(mID, tokens[1]);
            }
        } catch (FileNotFoundException e) {
            System.out.println("Error reading " + new File("ml-100k/u.item").getAbsolutePath());
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("\nFound " + movieNames.size() + " Movies.\n");
        return movieNames;
    }

    static class PrintMovies implements Function1<Object, Object> {
        @Override
        public Object apply(Object val) {
            GenericRowWithSchema actVal = (GenericRowWithSchema)val;
            int movId = actVal.getInt(0);
            float movRating = actVal.getFloat(1);
            System.out.println(movieNames.get(movId) + " : " + movRating);
            return null;
        }
    }

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java MLLib example")
                //https://stackoverflow.com/a/57611341/850777
				// without it would cause "Exception in thread "main" org.apache.spark.sql.AnalysisException: Detected cartesian product for LEFT OUTER join between logical plans"
                .config("spark.sql.crossJoin.enabled", "true" )
                .getOrCreate();

        JavaRDD<MovieRating> ratingsRDD = spark.read()
                //protocols supported: file:/// s3n://, hdfs://
                .textFile("ml-100k/u.data.user0") //JDBC/Cassandra/HBase/Elasticserach/JSON/CSV
                .javaRDD()
                .map(MovieRating::parseRating);

        Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, MovieRating.class);
        ratings.cache();

        // Build the recommendation model using ALS on the training data
        ALS als = new ALS()
                .setMaxIter(5)
                .setRegParam(0.01)
                .setUserCol("userId")
                .setItemCol("movieId")
                .setRatingCol("rating");
        ALSModel model = als.fit(ratings);

        // Evaluate the model by computing the RMSE on the test data.
		// Root Mean Square Error (RMSE) is the standard deviation of the residuals (prediction errors). 
		// Residuals are a measure of how far from the regression line data points are; RMSE is a measure of how spread out these residuals are. 
		// In other words, it tells you how concentrated the data is around the line of best fit.
        model.setColdStartStrategy("drop");

        //Going to recommend for user 0
        Dataset<Row> user0Ratings = ratings.filter(col("userId").equalTo("0"));
        System.out.println("user0Ratings");
        user0Ratings.show();
//    user0Ratings
//   +-------+------+---------+------+
//   |movieId|rating|timestamp|userId|
//   +-------+------+---------+------+
//   |     50|     5|881250949|     0|
//   |    172|     5|881250949|     0|
//   |    133|     1|881250949|     0|
//   +-------+------+---------+------+


        // Find movies rated more than 100 times
        Dataset<Row> ratingCounts = ratings.groupBy("movieId").count().filter("count > 100");
        System.out.println("ratingCounts");
        ratingCounts.show();
//   ratingCounts
//   +-------+-----+
//   |movieId|count|
//   +-------+-----+
//   |    496|  231|
//   |    471|  221|
//   |    148|  128|

        //Construct a "test" dataframe for user 0 with every movie rated more than 100 times, lit(0) adds a 'userId' column with value 0
        Dataset<Row> testMoviesForUser0 = ratingCounts.select("movieId").withColumn("userId", lit(0));
        System.out.println("testMoviesForUser0");
        testMoviesForUser0.show();
//    testMoviesForUser0
//   +-------+------+
//   |movieId|userId|
//   +-------+------+
//   |    496|     0|
//   |    471|     0|
//   |    148|     0|
//   |    243|     0|

        //Run our model on that list of popular movies for user ID 0
        Dataset<Row> recommendations = model.transform(testMoviesForUser0);
        System.out.println("recommendations");
        recommendations.show();
//        recommendations
//   +-------+------+----------+
//   |movieId|userId|prediction|
//   +-------+------+----------+
//   |    148|     0|  3.670433|
//   |    471|     0|   3.79199|
//   |    496|     0|  2.093522|


        //Get the top 20 movies with the highest predicted rating for this user
        //topRecommendations = recommendations.sort(recommendations.prediction.desc()).take(20)
        Dataset<Row> userRecs = model.recommendForUserSubset(testMoviesForUser0, 20);
        System.out.println("userRecs");
        userRecs.show();
//        +------+--------------------+
//        |userId|     recommendations|
//        +------+--------------------+
//        |     0|[[987, 10.80352],...|
//        +------+--------------------+


        userRecs.foreach(v1 -> {
            System.out.println("recommendations: " + v1.get(1));
            WrappedArray warr = (WrappedArray) v1.get(1);
            warr.foreach(new PrintMovies());
        });

        Dataset<Row> users = ratings.select(als.getUserCol()).where(col("userId").equalTo("0"));
        Dataset<Row> userSubsetRecs = model.recommendForUserSubset(users, 10);
        System.out.println("userSubsetRecs");
        userSubsetRecs.show();
        userSubsetRecs.foreach(genericRowWithSchema -> {
            ((WrappedArray)genericRowWithSchema.get(1)).foreach(new PrintMovies());
        });

        spark.stop();
    }
}
