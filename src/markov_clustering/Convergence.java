package markov_clustering;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Convergence {
	public static enum ConvergenceCounter{
		NOT_CONVERGED;	
	}
	public static boolean run(Configuration conf, Path oldMatrix, Path newMatrix) throws IOException, ClassNotFoundException, InterruptedException {
		Job convergence = Job.getInstance(conf, "MatrixConvergenceChecker");
		
        convergence.setJarByClass(Driver.class);
        convergence.setOutputKeyClass(Text.class);
        convergence.setOutputValueClass(DoubleWritable.class);
        convergence.setMapperClass(ConvergenceMapper.class);
        convergence.setReducerClass(ConvergenceReducer.class);
        convergence.setInputFormatClass(TextInputFormat.class);
        convergence.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(convergence, oldMatrix, TextInputFormat.class, ConvergenceMapper.class);
        MultipleInputs.addInputPath(convergence, newMatrix, TextInputFormat.class, ConvergenceMapper.class);
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-s");
		Calendar cal = Calendar.getInstance();
		Path convergenceOutput= new Path("/tmp/convergence"+dateFormat.format(cal.getTime()));
        FileOutputFormat.setOutputPath(convergence, convergenceOutput);
        convergence.submit();
        if (!convergence.waitForCompletion(true)) System.exit(-1);       
        boolean converged =  convergence.getCounters().findCounter(ConvergenceCounter.NOT_CONVERGED).getValue() <= 0; 
        System.out.println(convergence.getCounters().findCounter(ConvergenceCounter.NOT_CONVERGED).getValue());
        
        if (!converged) System.out.println("Max difference: "+ conf.getDouble("maxDifference", 0.0));
        
        return converged;
   
	}
}
