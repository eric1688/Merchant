package Test;

import java.io.IOException;
import java.util.regex.Pattern;



import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;



public class SourcedataToUSPrefMapper extends
		Mapper<LongWritable, Text, VLongWritable, VLongWritable> {

	

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		 String[] tokens = value.toString().split(",");

		 
		 if ( tokens.length < 75)  return;

		 String tokenUser=tokens[22].substring(1,tokens[22].length()-1).trim();
		 String tokenItem=tokens[54].substring(1,tokens[54].length()-1).trim();
		 //20 ,23
		 //String usercomplie="^[0-9]*$";
		 String usercomplie="^\\d{17}$";
		 String itemcomplie="^\\d{15}$";
		 if(Pattern.matches(usercomplie, tokenUser)&&Pattern.matches(itemcomplie, tokenItem)){
			
			  long userID = Long.parseLong(tokenUser);
			// double userID=Double.parseDouble(tokenUser);
			// String userID=tokenUser;
			long itemID = Long.parseLong(tokenItem);

		context.write(new VLongWritable(userID), new VLongWritable(itemID));
		}
	}

}
