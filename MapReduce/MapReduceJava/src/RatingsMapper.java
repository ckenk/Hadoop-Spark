
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RatingsMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context output) throws IOException,
                InterruptedException {
            //System.out.println("map key: " + key.toString());
            //System.out.println("map val: " + value.toString());

            //[userID, movieID, rating, timestamp]
            String[] words = value.toString().split("\\t");

            output.write(new Text(words[2]), one);
        }
}
