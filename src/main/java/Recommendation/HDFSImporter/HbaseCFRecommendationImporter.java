package Recommendation.HDFSImporter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.common.HadoopUtil;

import CFRecommendation.CFRecommenderDriver;
import CFRecommendation.CFRecommenderJob;
import CFRecommendation.DataProcessing.step0.ItemIDIndexMapper;
import Recommendation.HDFS.Util;


public class HbaseCFRecommendationImporter {

	
	//public void map()
	static class HbaseCFRecommendationMapper extends Mapper< LongWritable, Text,ImmutableBytesWritable, Put>{
		//private NcdcRecorderParser parser=new NcdcRecordParser();
		//System.out.println("key "+ tokens[0]);
		//System.out.println("value "+ tokens[1]);
		private HTable table;
		
		public void map(LongWritable key,Text value,Context context) throws IOException{
			configure(context.getConfiguration());
			String[] tokens=value.toString().split("\t");
			
			Put put=new Put(Bytes.toBytes(tokens[0].toString()));
			put.add(Bytes.toBytes("recommend"),Bytes.toBytes("data2"),Bytes.toBytes(tokens[1].toString()));
			table.put(put);
			close();
		}
		public  void configure(Configuration conf){
			try{
				//Configuration conf=HBaseConfiguration.create();
				conf.set("hbase.zookeeper.quorum", "192.168.75.133"); 
				this.table=new HTable(conf,"CFRecommendation");
			}catch(IOException e){
				throw new RuntimeException("Failed HTable constrtuction",e);
			}
		}
		public void close()throws IOException{
		//	super.close();
			table.close();
		}
	}
	
	//public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf0 = new Configuration();
		Job job0 = new Job(conf0, "step0");
		job0.setJarByClass(HbaseCFRecommendationImporter.class);
		job0.setMapperClass(HbaseCFRecommendationMapper.class);
		//job0.setReducerClass(Reducer.class);

		job0.setMapOutputKeyClass(LongWritable.class);
		job0.setMapOutputValueClass(Text.class);
		job0.setNumReduceTasks(0);
	//	job0.setOutputKeyClass(IntWritable.class);
	//	job0.setOutputValueClass(VLongWritable.class);

		FileInputFormat.addInputPath(job0, new Path(CFRecommenderJob.OutPutPath));
	//	Path outPath0 = new Path(Util.HDFS + "/CFRecommender/tmp/index");
	//	FileOutputFormat.setOutputPath(job0, outPath0);
		
		job0.setOutputFormatClass(NullOutputFormat.class);
		//job0.waitForCompletion(true);
		//return 0;
		   System.out.println(job0.waitForCompletion(true) ? 0 : 1);
	}

}
