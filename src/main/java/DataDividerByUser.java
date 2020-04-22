import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
	public static class DataDividerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//input user,movie,rating
			String[] user_movie_rating = value.toString().trim().split(",");
			String userID = user_movie_rating[0];
			String movieID = user_movie_rating[1];
			String rating = user_movie_rating[2];

			RawDataMap.movieToUserRating.putIfAbsent(movieID, new ArrayList<>());
			RawDataMap.movieToUserRating.get(movieID).add(userID + ":" + rating);

			RawDataMap.userToMovieRating.putIfAbsent(userID, new ArrayList<>());
			RawDataMap.userToMovieRating.get(userID).add(movieID + ":" + rating);
			//context.write(new IntWritable(userID), new Text(movieID + ":" + rating));
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Map.Entry<String, List<String>> entry : RawDataMap.userToMovieRating.entrySet()) {
				List<String> movieRatings = entry.getValue();
					for(int i = 0; i < movieRatings.size() && i < 30; i++) {
						String movie1 = movieRatings.get(i).trim().split(":")[0];
						for(int j = 0; j < movieRatings.size() && i < 30; j++) {
							String movie2 = movieRatings.get(j).trim().split(":")[0];
							context.write(new Text(movie1 + ":" + movie2), new IntWritable(1));
						}
					}



			}
		}
	}

	public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//key movie1:movie2 value = iterable<1, 1, 1>
			int sum = 0;
			while(values.iterator().hasNext()) {
				sum += values.iterator().next().get();
			}

			context.write(key, new IntWritable(sum));
		}
	}

//	public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
//		// reduce method
//		@Override
//		public void reduce(IntWritable key, Iterable<Text> values, Context context)
//				throws IOException, InterruptedException {
//
//			StringBuilder sb = new StringBuilder();
//			while (values.iterator().hasNext()) {
//				sb.append("," + values.iterator().next());
//			}
//			//key = user value=movie1:rating, movie2:rating...
//			context.write(key, new Text(sb.toString().replaceFirst(",", "")));
//		}
//	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(DataDividerMapper.class);
		job.setReducerClass(MatrixGeneratorReducer.class);

		job.setJarByClass(DataDividerByUser.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
