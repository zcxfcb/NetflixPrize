import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Preprocessor {
	public static class PreprocessorMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
         private String userID = null;
		// map method
		@Override
		public void cleanup(Context context
		) throws IOException, InterruptedException {
			System.out.print("I'm a mapper");
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//input user,movie,rating
			String valStr = value.toString();
			String[] valStrArr = valStr.split(",");
			if(valStr.charAt(valStr.length()-1) == ':') {
				this.userID = valStr.substring(0, valStr.length()- 1);
			} else if (valStrArr.length == 3) {
				String movieID = valStrArr[0];
				String rating = valStrArr[1];
				String date = valStrArr[2];
				context.write(NullWritable.get(), new Text(userID + "," + movieID + "," + rating));
			} else {
				throw new InterruptedException("Invalid line of:" + valStr);
			}

		}
	}

//	public static class PreprocessorReducer extends Reducer<Text, Text, Text, Text> {
//		// reduce method
//		@Override
//		public void reduce(Text key, Iterable<Text> values, Context context)
//				throws IOException, InterruptedException {
//
//			StringBuilder sb = new StringBuilder();
//			while (values.iterator().hasNext()) {
//				sb.append("," + values.iterator().next());
//			}
//			//key = movie value=user1:rating, user2:rating...
//			context.write(key, new Text(sb.toString().replaceFirst(",", "")));
//		}
//	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(PreprocessorMapper.class);
//		job.setReducerClass(PreprocessorReducer.class);
//		job.setNumReduceTasks(1);

		job.setJarByClass(Preprocessor.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
