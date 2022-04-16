import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

/**
 * Resources:
 *      https://codereview.stackexchange.com/questions/56641/producing-a-sorted-wordcount-with-spark  ***
 *
 *      https://stackoverflow.com/questions/29003246/how-to-achieve-sort-by-value-in-spark-java *
 *          "there is no specific API to sort the data on value." (in Spark Java)
 *
 *      https://www.tabnine.com/code/java/classes/scala.Tuple2
 *      https://stackoverflow.com/questions/31005751/top-is-not-functioning-with-javapairrdd-in-apache-spark
 */
public class WorstMovieSpark implements Serializable {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final Pattern TAB = Pattern.compile("\\t");

    static class DoubleIntegerComaprator implements Serializable, Comparator {
        //Reverse Comparison order
        @Override
        public int compare(Object o11, Object o12) {
            Tuple2<Double, Integer> o1 = (Tuple2<Double, Integer>) o11;
            Tuple2<Double, Integer> o2 = (Tuple2<Double, Integer>) o12;
            if (o1._1 > o2._1) return -1;
            if (o1._1 == o2._1) return 0;
            else return 1;
        }
    }

    private static Map<Integer, String> loadMovieNames() {
        HashMap<Integer, String> movieNames = new HashMap<>();
        int counter = 0;
        try (Scanner scanner = new Scanner(new File("ml-100k/u.item"), StandardCharsets.UTF_8.name())) {
            while (scanner.hasNext()) {
                String[] tokens = scanner.nextLine().split("\\|");
                Integer mID = Integer.valueOf(tokens[0]);
                if (counter++ < 100)System.out.println("" + mID + "|" + tokens[1]);
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

    public static void main(String[] args) throws Exception {

//        if (args.length < 1) {
//            System.err.println("Usage: JavaWordCount <file>");
//            System.exit(1);
//        }

        Map<Integer, String> movieNames = loadMovieNames();

        SparkSession spark = SparkSession
                .builder()
                .appName("WorstMoviesJavaSpark")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile("hdfs:///user/maria_dev/ml-100k/u.data").javaRDD();
        System.out.println("10 of lines read: " + lines.take(10));

        // Convert to (movieID, (rating, 1.0))
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> rdd_movieRatings = lines.mapToPair(s -> {
            String[] movieData = TAB.split(s);
            return new Tuple2<>(Integer.valueOf(movieData[1]), new Tuple2<>(Integer.valueOf(movieData[2]), 1));
        });
        System.out.println("rdd_movieRatings: " + rdd_movieRatings.take(100));

        //Reduce to (movieID, (sumOfRatings, numOfRatings)) -> movie1 & movie2 are 2 ratings of the same movie, key: movieID will be retained automatically
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> rdd_ratingTotalsAndCount = rdd_movieRatings.reduceByKey((movie1, movie2) ->
                new Tuple2<>(movie1._1() + movie2._1(), movie1._2() + movie2._2())
        );
        System.out.println("rdd_ratingTotalsAndCount: " + rdd_ratingTotalsAndCount.take(100));

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> popularTotalsAndCount = rdd_ratingTotalsAndCount.filter(integerTuple2Tuple2 -> integerTuple2Tuple2._2._2 > 10);


        // Map to (movieID, averageRating), key: movieID will be retained automatically
        //JavaPairRDD<Integer, Double> rdd_averageRatings = rdd_ratingTotalsAndCount.mapValues(totalAndCount -> new Double(totalAndCount._1() / (double)totalAndCount._2()));
        JavaPairRDD<Integer, Double> rdd_averageRatings = popularTotalsAndCount.mapValues(totalAndCount -> new Double(totalAndCount._1() / (double)totalAndCount._2()));
        System.out.println("rdd_averageRatings: " + rdd_averageRatings.take(100));

        // There is no way to sort by "value" in Java
        //Comparator<Tuple2<Integer, Double>> tupleComparator = Comparator.comparing(tuple2 -> tuple2._2);
        // Sort by average rating in ASC order, 1: 2nd field - averageRating
        //JavaPairRDD<Integer, Double> rdd_sortedMovies = rdd_averageRatings.sortByKey(Comparator.comparing(tuple2 -> tuple2._2)).collect();

        JavaPairRDD<Double, Integer> rdd_swappedRatings = rdd_averageRatings.mapToPair(integerDoubleTuple2 -> new Tuple2<>(integerDoubleTuple2._2(), integerDoubleTuple2._1()));
        System.out.println("rdd_swappedRatings: " + rdd_swappedRatings.take(100));
//        JavaPairRDD<Double, Integer> rdd_swappedRatingsSorted  = rdd_swappedRatings.sortByKey(false);
        JavaPairRDD<Double, Integer> rdd_swappedRatingsSorted  = rdd_swappedRatings.sortByKey();

        //System.out.println("rdd_swappedRatingsSorted: " + rdd_swappedRatingsSorted.takeOrdered(100, new DoubleIntegerComaprator()));
        System.out.println("rdd_swappedRatingsSorted: " + rdd_swappedRatingsSorted.top(100, new DoubleIntegerComaprator()));

        List<Tuple2<Double, Integer>> rdd_worstTen0 = rdd_swappedRatingsSorted.take(100);
        for (Tuple2<Double, Integer> worstTuple : rdd_worstTen0) {
            System.out.print(worstTuple + ", " );
        }
        //List<Tuple2<Double, Integer>> rdd_worstTen = rdd_swappedRatingsSorted.takeOrdered(100, new DoubleIntegerComaprator());
        List<Tuple2<Double, Integer>> rdd_worstTen = rdd_swappedRatingsSorted.top(100, new DoubleIntegerComaprator());
        System.out.printf("\n\n%9s | %-60s |                   %1s%n", "ID", "Title", "Rating");
        System.out.println("=====================================================================================================");
        for (Tuple2<Double, Integer> worstTuple : rdd_worstTen) {
            System.out.printf("%10d  %-80s : %1s%n", worstTuple._2, movieNames.get(worstTuple._2), worstTuple._1);
        }

        spark.stop();
    }
}
