package PFPGrowthRecommendation.Util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;








import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;

import com.google.common.collect.Lists;

import PFPGrowthRecommendation.PFPGrowthJob;
import Recommendation.HDFS.Util;

public class FPUtil {
	

	public static List<Pair<String, Long>> readFList(Configuration conf)throws IOException
		  {
		    List list = new ArrayList();
		   FileSystem fs=FileSystem.get(URI.create(Util.HDFS),conf);
		   Path fList=new Path(PFPGrowthJob.SavefListPath);
		    SequenceFile.Reader reader=new SequenceFile.Reader(fs, fList, conf);
		  try{
			  Text key=(Text)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			  LongWritable value=(LongWritable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			  while(reader.next(key,value)){
				  list.add(new Pair(key.toString(),Long.valueOf(value.get())));
				//  System.out.println("key "+key.toString()+" value "+Long.valueOf(value.get()));
			  }
		  }finally{
			  IOUtils.closeStream(reader);
		  }
		    return list;
		  }
	

}
