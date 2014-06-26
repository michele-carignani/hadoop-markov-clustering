package markov_clustering.test;

import java.io.IOException;

import markov_clustering.Driver;
import markov_clustering.MatrixRowMapper;
import markov_clustering.SumRReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StochasticRowVerifier {
	public static void run (Configuration configuration, Path input, Path output) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(configuration, "Matrix correctness verifier");
	    job.setJarByClass(Driver.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setReducerClass(CheckStochasticReducer.class);
	    job.setMapperClass(MatrixRowMapper.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
	    job.submit();
	    if(!job.waitForCompletion(true)) System.exit(-1);
	}
}
