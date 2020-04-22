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

public class DataDividerByUser {
	public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//input format:user,movie,rating
			String[] userMovieRating = value.toString().trim().split(",");
			int userID = Integer.parseInt(userMovieRating[0]);
			context.write(new IntWritable(userID), new Text(String.format("%s:%s", userMovieRating[1], userMovieRating[2])));
		}
	}

	public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		// reduce method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();
			PriorityQueue<String> top5 = new PriorityQueue<>((x, y) -> {
				String[] arrX = x.split(":");
				String[] arrY = y.split(":");
				return (int) Math.round(10 * (Double.valueOf(arrX[1]) - Double.valueOf(arrY[1])));
			});
			while (values.iterator().hasNext()) {
				top5.add(values.iterator().next().toString());
				while (top5.size() > 5) {
					top5.poll();
				}
			}

			while (!top5.isEmpty()) {
				sb.append("," + top5.poll());
			}
			//key = user; value=movie1:rating, movie2:rating...
//			System.out.println(key.toString() + "," + sb.substring(1));
			context.write(key, new Text(sb.substring(1)));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(DataDividerMapper.class);
		job.setReducerClass(DataDividerReducer.class);

		job.setJarByClass(DataDividerByUser.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
