package PFPGrowthRecommendation.step3;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.mahout.common.Pair;
import org.apache.mahout.fpm.pfpgrowth.TransactionTree;
import org.apache.mahout.math.list.IntArrayList;

public class ParallelFPGrowthCombiner extends Reducer<IntWritable, TransactionTree, IntWritable, TransactionTree>
{
  protected void reduce(IntWritable key, Iterable<TransactionTree> values, Reducer<IntWritable, TransactionTree, IntWritable, TransactionTree>.Context context)
    throws IOException, InterruptedException
  {
    TransactionTree cTree = new TransactionTree();
    for (TransactionTree tr : values) {
      for (Pair p : tr) {
        cTree.addPattern((IntArrayList)p.getFirst(), ((Long)p.getSecond()).longValue());
      }
    }
    context.write(key, cTree.getCompressedTree());
  }
}