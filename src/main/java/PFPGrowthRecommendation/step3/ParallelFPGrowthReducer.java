package PFPGrowthRecommendation.step3;

import PFPGrowthRecommendation.PFPGrowthDriver;
import PFPGrowthRecommendation.PFPGrowthJob;
import PFPGrowthRecommendation.Util.FPUtil;
import Recommendation.HDFS.Util;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.Pair;
import org.apache.mahout.fpm.pfpgrowth.CountDescendingPairComparator;
import org.apache.mahout.fpm.pfpgrowth.TransactionTree;
import org.apache.mahout.fpm.pfpgrowth.convertors.ContextStatusUpdater;
import org.apache.mahout.fpm.pfpgrowth.convertors.ContextWriteOutputCollector;
import org.apache.mahout.fpm.pfpgrowth.convertors.integer.IntegerStringOutputConverter;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;
import org.apache.mahout.fpm.pfpgrowth.fpgrowth.FPGrowth;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.list.LongArrayList;

public class ParallelFPGrowthReducer extends Reducer<IntWritable, TransactionTree, Text, TopKStringPatterns>
{
  private final List<String> featureReverseMap;
  private final LongArrayList freqList;
  private int maxHeapSize;
  private int minSupport;
  private int numFeatures;
  private int maxPerGroup;


  public ParallelFPGrowthReducer()
  {
    this.featureReverseMap = Lists.newArrayList();
    this.freqList = new LongArrayList();

    this.maxHeapSize = 50;

    this.minSupport = 3;
  }

  protected void reduce(IntWritable key, Iterable<TransactionTree> values, Reducer<IntWritable, TransactionTree, Text, TopKStringPatterns>.Context context)
    throws IOException
  {
    TransactionTree cTree = new TransactionTree();
    for (TransactionTree tr : values) {
      for (Pair p : tr) {
        cTree.addPattern((IntArrayList)p.getFirst(), ((Long)p.getSecond()).longValue());
      }
    }

    List localFList = Lists.newArrayList();
    for (Map.Entry fItem : cTree.generateFList().entrySet()) {
      localFList.add(new Pair(fItem.getKey(), ((MutableLong)fItem.getValue()).toLong()));
    }

    Collections.sort(localFList, new CountDescendingPairComparator());

   
      FPGrowth fpGrowth = new FPGrowth();
      fpGrowth.generateTopKFrequentPatterns(new IteratorAdapter(cTree.iterator()), localFList, this.minSupport, this.maxHeapSize, new HashSet(PFPGrowthDriver.getGroupMembers(key.get(), this.maxPerGroup, this.numFeatures).toList()), new IntegerStringOutputConverter(new ContextWriteOutputCollector(context), this.featureReverseMap), new ContextStatusUpdater(context));
    
  }

  protected void setup(Reducer<IntWritable, TransactionTree, Text, TopKStringPatterns>.Context context)
    throws IOException, InterruptedException
  {
    super.setup(context);
     //Parameters params = new Parameters(context.getConfiguration().get("pfp.parameters", ""));

    for (Pair e : FPUtil.readFList(context.getConfiguration())) {
      this.featureReverseMap.add(e.getFirst().toString());
      this.freqList.add(((Long)e.getSecond()).longValue());
    }

    this.maxHeapSize = Integer.valueOf(50).intValue();
    this.minSupport = PFPGrowthJob.minSupport;

    this.maxPerGroup = PFPGrowthDriver.maxPerGroup;
    this.numFeatures = this.featureReverseMap.size();
   // this.useFP2 = "true".equals(params.get("use_fpg2"));
  }

  private static class IteratorAdapter
    implements Iterator<Pair<List<Integer>, Long>>
  {
    private Iterator<Pair<IntArrayList, Long>> innerIter;

    private IteratorAdapter(Iterator<Pair<IntArrayList, Long>> transactionIter)
    {
      this.innerIter = transactionIter;
    }

    public boolean hasNext()
    {
      return this.innerIter.hasNext();
    }

    public Pair<List<Integer>, Long> next()
    {
      Pair innerNext = (Pair)this.innerIter.next();
      return new Pair(((IntArrayList)innerNext.getFirst()).toList(), innerNext.getSecond());
    }

    public void remove()
    {
      throw new UnsupportedOperationException();
    }
  }
}