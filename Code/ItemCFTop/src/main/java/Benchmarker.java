import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Compares the predicted result with the actual user rating
 */
public class Benchmarker {

  public static class RealDataMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split(",");
      String outputKey = line[0] + ":" + line[1];
      context.write(new Text(outputKey), new Text("real:" + line[2]));
    }
  }

  public static class PredictionMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split("\t");
      context.write(new Text(line[0]), new Text("predicted:" + line[1]));
    }
  }

  public static class BenchmarkingReducer extends Reducer<Text, Text, Text, Text> {

    private Double squareSum = 0.0;
    private Integer count = 0;
    private Integer precision = 100000;

    @Override
    public void reduce(Text key, Iterable<Text>values, Context context) throws IOException, InterruptedException {
      double realRating = 0, predictedRating = 0;
      int cnt = 0;
      for (Text value : values) {
        cnt++;
        String[] rating = value.toString().split(":");
        if (rating[0].equals("real")) {
          realRating = Double.parseDouble(rating[1]);
        } else {
          predictedRating = Double.parseDouble(rating[1]);
        }
      }
      if (cnt == 2) {
        squareSum += Math.pow(realRating - predictedRating, 2);
        count++;
      } else if (cnt > 2) {
        throw new InterruptedException("more than 2 record found for:" + key);
      }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      System.out.println("variance for Item_CF:" + squareSum/count);
      long convertedSquareSum = (long) (squareSum * precision);
      context.getCounter(Counters.MEAN_DIFF_SQUARE_SUM)
          .increment(convertedSquareSum);
      context.getCounter(Counters.RECORD_COUNT)
          .increment(count);
      context.write(new Text(squareSum.toString()), new Text(count.toString()));
    }
  }

  public static void main (String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf);

    ChainMapper.addMapper(job, RealDataMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
    ChainMapper.addMapper(job, PredictionMapper.class, Text.class, Text.class, Text.class, Text.class, conf);
    job.setMapperClass(RealDataMapper.class);
    job.setMapperClass(PredictionMapper.class);
    job.setReducerClass(BenchmarkingReducer.class);
    job.setNumReduceTasks(1);

    job.setJarByClass(Benchmarker.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RealDataMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PredictionMapper.class);

    TextOutputFormat.setOutputPath(job, new Path(args[2]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
