import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RatingsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context output)
                throws IOException, InterruptedException {
            int ratingCount = 0;
            for(IntWritable value: values){
                ratingCount += value.get();
            }
            //System.out.println("ratingCount = " + ratingCount);
            output.write(key, new IntWritable(ratingCount));
        }
}
