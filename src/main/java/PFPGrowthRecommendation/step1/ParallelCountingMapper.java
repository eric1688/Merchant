package PFPGrowthRecommendation.step1;


import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.Parameters;

public class ParallelCountingMapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
  private static final LongWritable ONE = new LongWritable(1L);
  private Pattern splitter;

  protected void map(LongWritable offset, Text input, Context context)
    throws IOException, InterruptedException
  {

    String[] items = this.splitter.split(input.toString());
    for (String item : items)
      if (!item.trim().isEmpty())
      {
    	 // System.out.println("item "+item);
        //context.setStatus("Parallel Counting Mapper: " + item);
        context.write(new Text(item), ONE);
      }
  }

  protected void setup(Context context) throws IOException, InterruptedException
  {
    super.setup(context);
    //Parameters params = new Parameters(context.getConfiguration().get("pfp.parameters", ""));
    //PFPGrowth.SPLITTER.toString()
    this.splitter = Pattern.compile(",");
  }
}
