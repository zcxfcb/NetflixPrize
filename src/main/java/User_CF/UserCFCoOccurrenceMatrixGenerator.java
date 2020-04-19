import java.io.IOException;

import java.util.HashMap;
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

/**
 * This class describes the relationship between each user. The benchmark is the common movies they
 * have watched. The more movies they have watched, the more similar of the two user(we ignore their
 * preferences toward the movie at this step).
 */
public class UserCFCoOccurrenceMatrixGenerator {
	public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		Map<String, List<String>> movieToUsers;

		private Map initializeMap() {
			if (movieToUsers == null) {
				movieToUsers = new HashMap<>();
			}
			return movieToUsers;
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//value = userid \t movie1: rating, movie2: rating...
			//key = movie1: movie2 value = 1
			String line = value.toString().trim();
			String[] movie_userRatings = line.split("\t");
			String[] user_ratings = movie_userRatings[1].split(",");

			//{movie1:rating, movie2:rating..}
			for(int i = 0; i < user_ratings.length; i++) {
				String user1 = user_ratings[i].trim().split(":")[0];
				
				for(int j = 0; j < user_ratings.length; j++) {
					String user2 = user_ratings[j].trim().split(":")[0];
					context.write(new Text(user1 + ":" + user2), new IntWritable(1));
				}
			}
			
		}
	}

	public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//key user1:user2 value = iterable<1, 1, 1>
			int sum = 0;
			while(values.iterator().hasNext()) {
				sum += values.iterator().next().get();
			}
			
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(MatrixGeneratorMapper.class);
		job.setReducerClass(MatrixGeneratorReducer.class);
		
		job.setJarByClass(UserCFCoOccurrenceMatrixGenerator.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
}
