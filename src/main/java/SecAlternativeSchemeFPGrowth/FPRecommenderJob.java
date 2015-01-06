package SecAlternativeSchemeFPGrowth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class FPRecommenderJob {
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(),
				new FPRecommenderDriver(), args));

	}
}
