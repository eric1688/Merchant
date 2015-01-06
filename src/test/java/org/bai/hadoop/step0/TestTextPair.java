package org.bai.hadoop.step0;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;



public class TestTextPair {
	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new Configuration(),
				new FPRecommenderDriver(), args));
	}
}
