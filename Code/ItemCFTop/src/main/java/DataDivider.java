import java.io.IOException;
import java.util.PriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class DataDivider {
	public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// parse input data
			String[] userMovieRating = value.toString().trim().split(",");
			int userID = Integer.parseInt(userMovieRating[0]);
			context.write(new IntWritable(userID), new Text(String.format("%s:%s", userMovieRating[1], userMovieRating[2])));
		}
	}

	public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// getting the top 20 ratings a user have
			StringBuilder sb = new StringBuilder();
			PriorityQueue<String> pq = new PriorityQueue<>((x, y) -> {
				String[] arrX = x.split(":");
				String[] arrY = y.split(":");
				return (int) Math.round(10 * (Double.valueOf(arrX[1]) - Double.valueOf(arrY[1])));
			});
			while (values.iterator().hasNext()) {
				pq.add(values.iterator().next().toString());
				while (pq.size() > 20) {
					pq.poll();
				}
			}

			while (!pq.isEmpty()) {
				sb.append("," + pq.poll());
			}
			context.write(key, new Text(sb.substring(1)));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(DataDividerMapper.class);
		job.setReducerClass(DataDividerReducer.class);

		job.setJarByClass(DataDivider.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
