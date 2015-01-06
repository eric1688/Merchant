package PFPGrowthRecommendation.step4;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.Pair;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;


public class AggregatorMapper extends Mapper<Text,TopKStringPatterns,Text,TopKStringPatterns> {
  
  @Override
  protected void map(Text key, TopKStringPatterns values, Context context) throws IOException,
                                                                          InterruptedException {
    for (Pair<List<String>,Long> pattern : values.getPatterns()) {
      for (String item : pattern.getFirst()) {
        List<Pair<List<String>,Long>> patternSingularList = Lists.newArrayList();
        patternSingularList.add(pattern);
       // context.setStatus("Aggregator Mapper:Grouping Patterns for " + item);
        context.write(new Text(item), new TopKStringPatterns(patternSingularList));
      }
    }
    
  }
}
