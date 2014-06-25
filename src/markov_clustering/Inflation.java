package markov_clustering;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Inflation {
	public static void run(Configuration configuration, Path input, Path output) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(configuration, "Matrix inflation");
	    job.setJarByClass(Driver.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    //FileSystem fs = FileSystem.get(conf);
	    job.setReducerClass(SumRReducer.class);
	     //job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
	    job.submit();
	    job.waitForCompletion(true);
	}
}
