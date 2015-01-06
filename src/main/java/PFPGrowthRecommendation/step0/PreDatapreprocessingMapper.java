package PFPGrowthRecommendation.step0;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
/*
 * 590/[22£º1.0£¬95£º2.0£¬...£¬9059£º3.0£¬...]
 */
public class PreDatapreprocessingMapper extends
Mapper<IntWritable, VectorOrPrefWritable, IntWritable, Text>{
	public void map(IntWritable key,VectorOrPrefWritable value,Context context)throws IOException,InterruptedException{
		Vector s1=value.getVector();
		System.out.println(value.toString());
		//s1.iterateNonZero();
		Iterator<Vector.Element> NonZeroIterator =value.getVector().iterateNonZero();
		while(NonZeroIterator.hasNext()){
			Vector.Element e = NonZeroIterator.next();
			int itemIndex = e.index();
			System.out.println("index "+itemIndex);
			context.write(key, new Text(itemIndex+""));
		}
	//	long userID=value.getUserID();
	//	 float userValue=value.getValue();
		//value.getValue();
		//value.getUserID();
//		System.out.println("userid"+userID+" uservalue "+userValue);
//		System.out.println("vector "+value.toString());
//		System.out.println(s1);
	//	context.write(key, new Text(s1.toString()));
	}
}
