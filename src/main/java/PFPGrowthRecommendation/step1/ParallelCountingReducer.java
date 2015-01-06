package PFPGrowthRecommendation.step1;


import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParallelCountingReducer extends Reducer<Text, LongWritable, Text, LongWritable>
{
  protected void reduce(Text key, Iterable<LongWritable> values, Context context)
    throws IOException, InterruptedException
  {
    long sum = 0L;
    for (LongWritable value : values) {
      //context.setStatus("Parallel Counting Reducer :" + key);
      sum += value.get();
    }
    //context.setStatus("Parallel Counting Reducer: " + key + " => " + sum);
    context.write(key, new LongWritable(sum));
  }
}