package markov_clustering;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Inflation {
		
	public static void run(Configuration configuration, Path input, Path output, int numWorkers) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(configuration, "Matrix inflation");
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
	    job.setJarByClass(Driver.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setReducerClass(SumRReducer.class);
	    job.setMapperClass(MatrixRowMapper.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		job.setNumReduceTasks(numWorkers);
	    job.submit();
	    if(!job.waitForCompletion(true)) System.exit(-1);	    
	}
}
