package markov_clustering.blockmultiplication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BlockWiseMatrixMultiplication extends Configured implements Tool {

	@Override
	/** arg[0] inputdir 1
	 *  arg[1] inputdir 2
	 *  arg[2] outputdir
	 */
	public int run(String[] args) throws Exception {
		Configuration conf = super.getConf();
		int matrixSplits = conf.getInt("splits", 10);
		for(int i = 0; i < matrixSplits; i++) {
			for (int j = 0; j < matrixSplits; j++) {
				ToolRunner.run(conf, new BlockMultiplier(), new String[]{args[0], args[1], args[2], i+"", j+""});
			}
		}
		return 0;
	}

}
