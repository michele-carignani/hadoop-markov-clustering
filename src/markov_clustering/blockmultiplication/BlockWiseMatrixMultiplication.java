package markov_clustering.blockmultiplication;

import markov_clustering.blockmultiplication.BlockMultiplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BlockWiseMatrixMultiplication extends Configured implements Tool {

	
	/** arg[0] inputdir 1
	 *  arg[1] inputdir 2
	 *  arg[2] outputdir
	 *  arg[3] number of parallel blocks multiplication
	 */
	private int blockRow = 0;
	private int blockColumn = 0;
	private int splits;
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = super.getConf();
		splits = conf.getInt("splits", 10);
		while(blockRow < splits) {
			ToolRunner.run(conf, new BlockMultiplier(calculatePartition(Integer.parseInt(args[3])), args[0], args[1], args[2], Integer.parseInt(args[3])), new String[]{});
		}
		return 0;
	}
	/** Calculate a number of blocks to be calculated of size given in input
	 * @param size the size of partition */
	public MatrixBlocks calculatePartition(int size) {
		MatrixBlocks set = new MatrixBlocks();
		while(size > 0 && blockRow < splits) {
			while(blockColumn < splits && size-- > 0) {
				set.addBlock(blockRow, blockColumn++);
			}
			if (size == 0) break;
			blockColumn = 0;
			blockRow++;
		}
		return set;
	}

}
