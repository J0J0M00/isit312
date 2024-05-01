import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class solution2 {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "solution 2");
        job.setJarByClass(solution2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(MinMaxTotalReducer.class);
        job.setReducerClass(MinMaxTotalReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text state = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                // Name of the state as first token
                state.set(itr.nextToken());

                // ignore the 2nd token
                itr.nextToken();

                // Accept the next token as rainfall amount
                int rainfall = Integer.parseInt(itr.nextToken());

                context.write(state, new IntWritable(rainfall));
            }
        }
    }

    public static class MinMaxTotalReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {

            int sum = 0;
            int min = Integer.MAX_VALUE;
            int max = Integer.MIN_VALUE;

            for (IntWritable val : values) {
                sum += val.get();

                if (val.get() < min) {
                    min = val.get();
                }

                if (val.get() > max) {
                    max = val.get();
                }
            }

            String resultString = String.format("%50s %5s %5s", sum, max, min);

            result.set(resultString);
            context.write(key, result);
        }
    }
}
