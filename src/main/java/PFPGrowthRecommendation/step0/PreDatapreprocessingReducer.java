package PFPGrowthRecommendation.step0;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PreDatapreprocessingReducer extends Reducer<IntWritable,Text ,NullWritable,Text>{
	public void reduce(IntWritable key,Iterable<Text> values,Context context)throws IOException, InterruptedException{
		StringBuilder sb=new StringBuilder();
		for(Text value :values){
			sb.append(value.toString()+",");
		}
		String append= sb.substring(0, sb.length()-1).toString();
		System.out.println("sb "+append);
		
		context.write( null, new Text(append));
	}
	
}
