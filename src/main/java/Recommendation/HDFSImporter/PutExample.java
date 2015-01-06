package Recommendation.HDFSImporter;


import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import Recommendation.HDFS.Util;



public class PutExample {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
//		Configuration conf=HBaseConfiguration.create();
//		//conf.set("hbase.master", "192.168.75.133:2181");
//		 conf.set("hbase.zookeeper.quorum", "192.168.75.133"); 
//		HTable table=new HTable(conf,"test_");
//		Put put=new Put(Bytes.toBytes("row3"));
//		put.add(Bytes.toBytes("cf"),Bytes.toBytes("c"),Bytes.toBytes("baby"));
//		table.put(put);
//		put.add(Bytes.toBytes("conlfaml"),Bytes.toBytes("qual1"),Bytes.toBytes("val1"));
//		put.add(Bytes.toBytes("colfam1"),Bytes.toBytes("qual2"),Bytes.toBytes("val2"));
//		
//		String[] cfs={"a","b","c","d","e","f"};
//		 for (int j =0; j < cfs.length; j++) {
//			              put.add(Bytes.toBytes(cfs[j]),
//			                      Bytes.toBytes(String.valueOf(1)),
//			                      Bytes.toBytes("value_1"));
//			              table.put(put);
//		 }            
		//table.put(put);
//		 HBaseAdmin admin = new HBaseAdmin(conf);
//		 HTableDescriptor tableDescriptor = admin.getTableDescriptor(Bytes.toBytes("test_"));
//		 byte[] name = tableDescriptor.getName();
//		 System.out.println(new String(name));
//		 HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
//		 for (HColumnDescriptor d : columnFamilies) {
//		 System.out.println(d.getNameAsString());
//		 }
		Configuration conf=HBaseConfiguration.create();
		//conf.set("hbase.master", "192.168.75.133:2181");
		conf.set("hbase.zookeeper.quorum", "192.168.75.133"); 
		HTable table=new HTable(conf,"CFRecommendation");
		Get get=new Get(Bytes.toBytes("3"));
		//get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("a"));
		Result result=table.get(get);
		byte[] value=result.getValue(Bytes.toBytes("recommend"), Bytes.toBytes("data"));
		System.out.println("value: "+Bytes.toString(value));
	}
	
}
