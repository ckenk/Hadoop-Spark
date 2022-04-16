import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RatingApplication  extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new RatingApplication(), args);
        System.out.println("run result = " + result);
        System.exit(result);
    }

    @Override
    public int run(String[] paths) throws Exception {
        if (paths.length != 2) {
            System.out.println("usage: [input_path] [output_path]");
            System.exit(-1);
        }
        Job job = Job.getInstance();
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(RatingsMapper.class);
        job.setReducerClass(RatingsReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(paths[0]));
        FileOutputFormat.setOutputPath(job, new Path(paths[1]));

        job.setJarByClass(RatingApplication.class);

        job.submit();

        return 0;
    }
}
