//package Recommendation.HDFSImporter;
//
//import java.io.IOException;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//
//import CFRecommendation.CFRecommenderDriver;
//import CFRecommendation.CFRecommenderJob;
//
//public class HbaseCFImporter {
//	 public static class MapperClass extends
//     Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
//   //ÁÐÃû
////   public static final String[] COLUMNS = { "card", "type",
////       "amount", "time", "many" };
// 
//   public void map(LongWritable key, Text value, Context context)
//       throws IOException, InterruptedException {
//     String[] cloumnVals = value.toString().split(",");
//     String rowkey = key.toString();
//     Put put = new Put(rowkey.getBytes());
//     for (int i = 0; i < cloumnVals.length; i++) {
//    	 //COLUMNS[i].getBytes(),
//       put.add("info".getBytes(), "recommedation".getBytes(),
//           cloumnVals[i].getBytes());
//     }
//     context.write(new ImmutableBytesWritable(rowkey.getBytes()), put);
//   }
// }
// 
// @SuppressWarnings("deprecation")
// public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//   Configuration conf = new Configuration();
////   conf.set("fs.defaultFS", "hdfs://ubuntu:9000/");
////   conf.set("mapreduce.framework.name", "local");
////   conf.set("mapred.job.tracker", "ubuntu:9001");
////   conf.set("hbase.zookeeper.quorum", "ubuntu");
//   Job job = new Job(conf,"CFHbase");
//   job.setJarByClass(HbaseCFImporter.class);
//   job.setMapperClass(MapperClass.class);
//   job.setNumReduceTasks(0);
//   job.setOutputFormatClass(TableOutputFormat.class);
//   job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "CFRecommendTable");
//   FileInputFormat.addInputPath(job, new Path(CFRecommenderJob.OutPutPath));
//   System.out.println(job.waitForCompletion(true) ? 0 : 1);
// 
//}
//}
