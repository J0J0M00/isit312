import java.io.IOException;
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

    // Ask Atharva IDK how mapper actually works
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text state = new Text();
        private Text cityWithMax = new Text();
        private Text cityWithMin = new Text();
        private IntWritable rainfall = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s+");
            state.set(tokens[0]);
            cityWithMax.set(tokens[1]);
            cityWithMin.set(tokens[1]);
            rainfall.set(Integer.parseInt(tokens[2]));
            context.write(state, rainfall);

            // Max Rainfall
            context.write(new Text(state.toString() + " MAX"), new IntWritable(rainfall.get()));

            // Min Rainfall
            context.write(new Text(state.toString() + " MIN"), new IntWritable(rainfall.get()));

        }
    }

    public static class RFStats {
        private int totalRainfall = 0;
        private int maxRainfall = Integer.MIN_VALUE;
        private int minRainfall = Integer.MAX_VALUE;
        private String CityMaxRain = "";
        private String CityMinRain = "";

        public void update(int rain, String city) {
            totalRainfall += rain;
            if (rain > maxRainfall) {
                maxRainfall = rain;
                CityMaxRain = city;
            }
            if (rain < minRainfall) {
                minRainfall = rain;
                CityMinRain = city;
            }
        }

        public String toString() {
            return totalRainfall + " " + maxRainfall + " " + CityMaxRain + " " + minRainfall + " " + CityMinRain;
        }
    }

    // Ask Atharva on reducer, not very sure what it does
    public static class RainfallReducer extends Reducer<Text, IntWritable, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            RainfallStats stats = new RFStats();
            String city = "";
            if (key.toString().endsWith("MAX")) {
                city = key.toString().replace(" MAX", "");
            } else if (key.toString().endsWith("MIN")) {
                city = key.toString().replace(" MIN", "");
            }
            for (IntWritable val : values) {
                stats.update(val.get(), city);
            }
            result.set(stats.toString());
            context.write(new Text(key.toString().replaceAll(" MAX| MIN", "")), result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Rainfall Analysis");
        job.setJarByClass(RainfallAnalysis.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(RainfallReducer.class);
        job.setReducerClass(RainfallReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
