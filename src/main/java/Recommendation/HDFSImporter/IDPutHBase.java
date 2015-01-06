package Recommendation.HDFSImporter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class IDPutHBase {
	public static String UserIDFind(long userID) throws IOException{
		
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.master", "192.168.75.133:2181");
		conf.set("hbase.zookeeper.quorum", "192.168.75.133"); 
		HTable table=new HTable(conf,"CFRecommendation");
		Get get=new Get(Bytes.toBytes(userID));
		Result result=table.get(get);
		byte[] value=result.getValue(Bytes.toBytes("recommend"), Bytes.toBytes("data"));
		System.out.println("value: "+Bytes.toString(value));
		return Bytes.toString(value);
	}
public static String ItemIDFind(long itemID) throws IOException{
		
		Configuration conf=HBaseConfiguration.create();
		//conf.set("hbase.master", "192.168.75.133:2181");
		conf.set("hbase.zookeeper.quorum", "192.168.75.133"); 
		HTable table=new HTable(conf,"PFPRecommendation");
		Get get=new Get(Bytes.toBytes(itemID));
		Result result=table.get(get);
		byte[] value=result.getValue(Bytes.toBytes("recommend"), Bytes.toBytes("data"));
		System.out.println("value: "+Bytes.toString(value));
		return Bytes.toString(value);
	}
}
