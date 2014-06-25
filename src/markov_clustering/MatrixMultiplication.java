package markov_clustering;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixMultiplication {
	public static void run(Configuration conf, Path original_data,Path input_path, Path output_path) throws IOException, ClassNotFoundException, InterruptedException {
		 Job job = Job.getInstance(conf, "MatrixMatrixMultiplicationTwoSteps");
	     job.setJarByClass(Driver.class);
	     job.setOutputKeyClass(Text.class);
	     job.setOutputValueClass(Text.class);
	     FileSystem fs = FileSystem.get(conf);
	     job.setReducerClass(MatrixRowByColumnReducer.class);
	     //job.setInputFormatClass(TextInputFormat.class);
	     job.setOutputFormatClass(TextOutputFormat.class);
	     /** Original data contains the original matrix m0 which is never changed */
	     MultipleInputs.addInputPath(job, original_data, TextInputFormat.class, MatrixRowMapper.class);
	     /** input path contains the current matrix achieved in the multiplication process */
        MultipleInputs.addInputPath(job, input_path, TextInputFormat.class, MatrixColumnMapper.class);
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
		 Calendar cal = Calendar.getInstance();
		 Path secondStepOutput= new Path("/tmp/mul2tmp"+dateFormat.format(cal.getTime()));
	     FileOutputFormat.setOutputPath(job, secondStepOutput);
	     job.submit();
	     if(!job.waitForCompletion(true)) System.exit(-1);
	     Job job2 = Job.getInstance(conf, "MatrixMatrixMultiplicationTwoSteps 2");
	     
	     job2.setJarByClass(Driver.class);
	     job2.setOutputKeyClass(Text.class);
	     job2.setOutputValueClass(DoubleWritable.class);
	 
	     job2.setMapperClass(IdentityMapperForDoubles.class);
	     job2.setReducerClass(MatrixSumReducer.class);
	     job2.setInputFormatClass(TextInputFormat.class);
	     job2.setOutputFormatClass(TextOutputFormat.class);
	     FileInputFormat.addInputPath(job2, secondStepOutput);
	     
	     FileOutputFormat.setOutputPath(job2, output_path);
	     job2.submit();
	     if(!job2.waitForCompletion(true)) System.exit(-1);
	     fs.delete(secondStepOutput, true);
	}
}
