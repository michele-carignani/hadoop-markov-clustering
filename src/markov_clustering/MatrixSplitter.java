package markov_clustering;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

/** 
 *  Splits a matrix into separate blocks.
 * */
public class MatrixSplitter extends Configured implements Tool {
	
	
	private int run(Configuration conf, Path input, Path output) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf, "Matrix Splitting");
		
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		job.setJarByClass(Driver.class);   
		job.setMapperClass(RowSplitter.class);
		job.setMapOutputKeyClass(Text.class);
		job.setReducerClass(RowSplitterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(2);
		if(!job.waitForCompletion(true)) System.exit(-1);
		return 1;
	}

	@Override
	public int run(String[] arg0) throws Exception {
		return run(getConf(), new Path(arg0[0]), new Path(arg0[1]));
	}
		
}