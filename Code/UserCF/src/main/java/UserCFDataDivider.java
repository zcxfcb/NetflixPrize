import java.io.IOException;
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
 * Parse input dataset and emit output for CoOccurrenceMatrixProducer
 */
public class UserCFDataDivider {
	public static class DataDividerMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//parse input data
			String[] line = value.toString().trim().split(",");
			String userID = line[0];
			String movieID = line[1];
			String rating = line[2];

			context.write(new Text(movieID), new Text(userID + ":" + rating));
		}
	}

	public static class DataDividerReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// limit the number of ratings to 20
			StringBuilder sb = new StringBuilder();
			int i = 0;
			while (i <= 20 && values.iterator().hasNext()) {
				i++;
				sb.append(",").append(values.iterator().next());
			}
			//key = movie value=user1:rating, user2:rating...
			context.write(key, new Text(sb.toString().replaceFirst(",", "")));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(DataDividerMapper.class);
		job.setReducerClass(DataDividerReducer.class);

		job.setJarByClass(UserCFDataDivider.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
