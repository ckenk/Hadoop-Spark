import org.apache.spark.mllib.recommendation.Rating;

import java.io.Serializable;

public class MovieRating implements Serializable {
    private int userId, movieId, rating;
    private long timestamp;

    public MovieRating(int userId, int movieId, int rating, long timestamp) {
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
        this.timestamp = timestamp;
    }

    public int getMovieId() {
        return this.movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    public int getRating() {
        return this.rating;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getUserId() {
        return this.userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public static MovieRating parseRating(String str) {
        String[] fields = str.split("\\t");
        if (fields.length != 4) {
            throw new IllegalArgumentException("Each line must contain 4 fields");
        }
        int userId = Integer.parseInt(fields[0]);
        int movieId = Integer.parseInt(fields[1]);
        int rating = Integer.parseInt(fields[2]);
        long timestamp = Long.parseLong(fields[3]);
        return new MovieRating(userId, movieId, rating, timestamp);
    }
}