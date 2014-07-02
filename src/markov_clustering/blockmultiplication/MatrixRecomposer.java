package markov_clustering.blockmultiplication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
/**
 * Recomposes a previously splitted matrix
 */
public class MatrixRecomposer extends Configured implements Tool {
	
	@Override
	/** args[0] the directory containing the matrix splitted in blocks to recompose
	 * args[1] the destination directory.
	 */
	public int run(String[] args) throws Exception {
		Configuration conf = super.getConf();
		Path input = new Path(args[0]+"/*/");
		Path output = new Path(args[1]);
		Job recomposer = Job.getInstance(conf, "Matrix Recomposition");
		recomposer.setInputFormatClass(TextInputFormat.class);
		recomposer.setOutputFormatClass(TextOutputFormat.class);
		recomposer.setOutputKeyClass(Text.class);
		recomposer.setOutputValueClass(DoubleWritable.class);
		recomposer.setMapperClass(RecomposerMapper.class);
		recomposer.setNumReduceTasks(0);
		FileInputFormat.addInputPath(recomposer, input);
		FileOutputFormat.setOutputPath(recomposer, output);
		return 0;
	}


}
