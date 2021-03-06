import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Normalizes the CoOccurrenceMatrix.
 */
public class UserCFNormalization {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //parse input data, which is in the form of "userA:userB \t relation"
            String[] user_relation = value.toString().trim().split("\t");
            String[] users = user_relation[0].split(":");

            context.write(new Text(users[0]), new Text(users[1] + ":" + user_relation[1]));
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //key = userA, value=<userB:relation, userC:relation...>
            int sum = 0;
            Map<String, Integer> map = new HashMap<String, Integer>();
            while (values.iterator().hasNext()) {
                String[] user_relation = values.iterator().next().toString().split(":");
                int relation = Integer.parseInt(user_relation[1]);
                sum += relation;
                map.put(user_relation[0], relation);
            }

            for(Map.Entry<String, Integer> entry: map.entrySet()) {
                String outputKey = entry.getKey();
                String outputValue = key.toString() + "=" + (double)entry.getValue() / sum;
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(UserCFNormalization.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
