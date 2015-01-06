package PFPGrowthRecommendation.step3;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.fpm.pfpgrowth.TransactionTree;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.math.set.OpenIntHashSet;

import PFPGrowthRecommendation.PFPGrowthDriver;
import PFPGrowthRecommendation.PFPGrowthJob;
import PFPGrowthRecommendation.Util.FPUtil;

public class ParallelFPGrowthMapper extends Mapper<LongWritable, Text, IntWritable, TransactionTree>
{
  private final OpenObjectIntHashMap<String> fMap = new OpenObjectIntHashMap();
  private Pattern splitter;
  private int maxPerGroup;
  private IntWritable wGroupID = new IntWritable();

  protected void map(LongWritable offset, Text input, Mapper<LongWritable, Text, IntWritable, TransactionTree>.Context context)
    throws IOException, InterruptedException
  {
    String[] items = this.splitter.split(input.toString());

    OpenIntHashSet itemSet = new OpenIntHashSet();

    for (String item : items) {
      if ((this.fMap.containsKey(item)) && (!item.trim().isEmpty())) {
        itemSet.add(this.fMap.get(item));
      }
    }

    IntArrayList itemArr = new IntArrayList(itemSet.size());
    itemSet.keys(itemArr);
    itemArr.sort();

    OpenIntHashSet groups = new OpenIntHashSet();
    for (int j = itemArr.size() - 1; j >= 0; j--)
    {
      int item = itemArr.get(j);
     // int groupID = PFPGrowth.getGroup(item, this.maxPerGroup);
       int groupID =  item / maxPerGroup;

      if (!groups.contains(groupID)) {
        IntArrayList tempItems = new IntArrayList(j + 1);
        tempItems.addAllOfFromTo(itemArr, 0, j);
        //context.setStatus("Parallel FPGrowth: Generating Group Dependent transactions for: " + item);
        this.wGroupID.set(groupID);
        context.write(this.wGroupID, new TransactionTree(tempItems, Long.valueOf(1L)));
      }
      groups.add(groupID);
    }
  }

  protected void setup(Mapper<LongWritable, Text, IntWritable, TransactionTree>.Context context)
    throws IOException, InterruptedException
  {
    super.setup(context);

    int i = 0;
  
   // List<Pair<Sring,Long>> fList=FPUtil.readFLists(context.getConfiguration());
   // Parameters params = new Parameters(context.getConfiguration().get("pfp.parameters", ""));
    for(Pair e: FPUtil.readFList(context.getConfiguration())){
    	this.fMap.put(e.getFirst().toString(), i++);
    }
    this.splitter = Pattern.compile(PFPGrowthJob.splitter);
    
    this.maxPerGroup = PFPGrowthDriver.maxPerGroup;
    
  }
}