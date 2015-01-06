package org.bai.hadoopTest;

import org.apache.mahout.fpm.pfpgrowth.FPGrowthDriver;

public class Test {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		String  [] arg ={"-s","5",
                "-i","hdfs://192.168.75.133:9000/user/orisun/input/data",
                "-o","hdfs://192.168.75.133:9000/user/orisun/output",
                "-k","4",
                "-g","50",
                "-method","mapreduce",
                "-e","UTF-8",
                "-tc","5"};
		FPGrowthDriver.main(arg);
	}
}
