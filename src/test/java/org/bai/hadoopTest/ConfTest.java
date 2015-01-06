package org.bai.hadoopTest;



import org.apache.hadoop.conf.Configuration;

public class ConfTest {
	public  static void main(String[] args){
		Configuration conf=new Configuration();
		conf.addResource("hadoop/core-site.xml");
		String color=conf.get("fs.default.name");
		System.out.println("fs.default.name "+color);
	}
}
