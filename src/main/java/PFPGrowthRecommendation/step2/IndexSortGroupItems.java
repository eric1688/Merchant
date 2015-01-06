package PFPGrowthRecommendation.step2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;

import PFPGrowthRecommendation.PFPGrowthJob;
import Recommendation.HDFS.Util;

import com.google.common.collect.Lists;

public class IndexSortGroupItems {
	public static List<Pair<String, Long>> readFList() throws IOException
	  {
	    int minSupport = PFPGrowthJob.minSupport;
	    Configuration conf = new Configuration();
	    FileSystem fs=FileSystem.get(URI.create(Util.HDFS),conf);
	    Path parallelCountingPath = new Path(PFPGrowthJob.CountingOutPath+"/part-r-00000");

	    Comparator<Pair> comp=new Comparator<Pair>() {

			public int compare(Pair o1, Pair o2) {
				int ret = ((Long)o2.getSecond()).compareTo((Long)o1.getSecond());
		        if (ret != 0) {
		          return ret;
				
		        }
		        return ((String)o1.getFirst()).compareTo((String)o2.getFirst());
			}
	    };
		PriorityQueue queue = new PriorityQueue(11, comp);
		FSDataInputStream in =fs.open(parallelCountingPath);
		InputStreamReader isr=new InputStreamReader(in);
		BufferedReader br=new BufferedReader(isr);
		try{
			String line;
			while((line=br.readLine())!=null){
				String[] str = line.split("\t");
				String word = str[0];
				Long value=Long.parseLong(str[1]);
				if(value>=minSupport){
					queue.add(new Pair(word,value));
				}
			}
		}finally{
			br.close();
//			isr.close();
//			in.close();
		}
		List fList=Lists.newArrayList();
		while(!queue.isEmpty()){
			fList.add(queue.poll());
		}
		return fList;
	  }

	public static void saveFList(Iterable<Pair<String, Long>> flist, Configuration conf)
		    throws IOException
		  {
		    Path flistPath = new Path(PFPGrowthJob.SavefListPath);
		    FileSystem fs = FileSystem.get(URI.create(Util.HDFS), conf);
		    flistPath = fs.makeQualified(flistPath);
		    HadoopUtil.delete(conf, new Path[] { flistPath });
		    SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, flistPath, Text.class, LongWritable.class);
		    try {
		      for (Pair pair : flist){
		        writer.append(new Text((String)pair.getFirst()), new LongWritable(((Long)pair.getSecond()).longValue()));
		      // 	System.out.println("first "+((String)pair.getFirst())+" long "+ pair.getSecond());
		      //  System.out.println(pair.toString());
		      }
		    }
		    finally {
		      writer.close();
		    }
		 //   DistributedCache.addCacheFile(flistPath.toUri(), conf);
		  }
}
