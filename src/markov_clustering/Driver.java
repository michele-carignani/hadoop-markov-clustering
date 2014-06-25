package markov_clustering;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {

	public static class MarkovConvergenceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	/** Format of the incoming file is row,column\t value */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] s = value.toString().split("\t");
			Text outKey = new Text(s[0]);
			DoubleWritable outVal = new DoubleWritable(Double.parseDouble(s[1]));
			context.write(outKey, outVal);
		}

	}
	
	public static class MarkovConvergenceReducer extends Reducer<Text,DoubleWritable,Text,Text> {
		
		public void reduce(Text coordinates, Iterable<DoubleWritable> values, Context context)
	            throws IOException, InterruptedException {
			

			
			Configuration conf = context.getConfiguration();
			
			double threeshold = conf.getDouble("threshold", 0.00001);
			
			Iterator<DoubleWritable> val = values.iterator();
			
			double prev, next;
			prev = (!val.hasNext()) ? 0.0 : val.next().get();
			next = (!val.hasNext()) ? 0.0 : val.next().get();
			
			double difference = Math.abs(prev-next);
			if(difference > threeshold) {
				context.getCounter(ConvergenceCounter.NOT_CONVERGED).increment(1);
				conf.setBoolean("converged", false);
				conf.setDouble("convergedValue", difference);
			} else {
				double maxdiff = conf.getDouble("maxDifference", 0.0);
				conf.setDouble("maxDifference", Math.max(difference, maxdiff));
			}
		
			context.write(coordinates, new Text(prev+"\t"+next));
		}
	}
	
	public static class IdentityMapperForDoubles extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		public void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException{
			String[] content = v.toString().split("\t");
			c.write(new Text(content[0]), new DoubleWritable(Double.parseDouble(content[1])));
			}
		}
		/** Format is row,column\t value */
	    public static class RowMap extends Mapper<LongWritable, Text, Text, Text> {
	        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	            String[] line = value.toString().split("\t");
	            String[] indicesAndValue = line[0].split(",");
	            Text outputKey = new Text();
	            Text outputValue = new Text();	            
	                outputKey.set(indicesAndValue[1]);
	                outputValue.set("A," + indicesAndValue[0] + "," + line[1]);
	                context.write(outputKey, outputValue);
	          
	        }
	    }
	    
	    public static class ColumnMap extends Mapper<LongWritable, Text, Text, Text> {
	        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	            String[] line = value.toString().split("\t");
	            String[] indicesAndValue = line[0].split(",");
	            Text outputKey = new Text();
	            Text outputValue = new Text();
	           	outputKey.set(indicesAndValue[1]);
	            outputValue.set("B," + indicesAndValue[1] + "," + line[1]);
	            context.write(outputKey, outputValue);
	            
	        }
	    }

	    public static class SecondStepReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	    	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
	    		double result = 0;
	    		Iterator<DoubleWritable> it = values.iterator();
	    		while(it.hasNext()) {
	    			result += it.next().get();
	    		}
	    		context.write(key, new DoubleWritable(result));
	    	}
	    }
	 
	    public static class FirstStepReduce extends Reducer<Text, Text, Text, DoubleWritable> {
	        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	            String[] value;
	            ArrayList<Entry<Integer, Float>> listA = new ArrayList<Entry<Integer, Float>>();
	            ArrayList<Entry<Integer, Float>> listB = new ArrayList<Entry<Integer, Float>>();
	            for (Text val : values) {
	                value = val.toString().split(",");
	                if (value[0].equals("A")) {
	                    listA.add(new SimpleEntry<Integer, Float>(Integer.parseInt(value[1]), Float.parseFloat(value[2])));
	                } else {
	                    listB.add(new SimpleEntry<Integer, Float>(Integer.parseInt(value[1]), Float.parseFloat(value[2])));
	                }
	            }
	            String i;
	            float a_ij;
	            String k;
	            float b_jk;
	            DoubleWritable outputValue;
	            Text out_key;
	            for (Entry<Integer, Float> a : listA) {
	                i = Integer.toString(a.getKey());
	                a_ij = a.getValue();
	                if (a_ij != 0){
		                for (Entry<Integer, Float> b : listB) {
		                    k = Integer.toString(b.getKey());
		                    b_jk = b.getValue();
		                    if(b_jk != 0){
			                    out_key = new Text(i + "," + k);
			                    outputValue = new DoubleWritable(a_ij * b_jk);
			                    context.write(out_key, outputValue);
		                    }
		                }
	                }
	            }
	        }
	    }
	
	
	/**
	 * @param args[0] input folder
	 * @param args[1] output folder
	 * @param arg[2] maximum number of iterations
	 * @param arg[3] number of workers (not mandatory; if not specified will be one for each row)
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Configuration clusterConf = new Configuration();
		int iterations = 0;
		int maxIterations = Integer.parseInt(args[2].trim());
		Path[] working = new Path[2];
		boolean converged = false;
		/** Directory for the original matrix M0 */
		Path inputfolder = new Path(args[0]);
		clusterConf.setDouble("threshold", 0.00001);
		clusterConf.setBoolean("converged", true);
		/** Working directory 1, at first step initialize with copy of original matrix M0 */
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
	    Calendar cal = Calendar.getInstance();
		working[0] = new Path("/tmp/intmul"+dateFormat.format(cal.getTime()));
		/** Working directory 2, at first step empty */
		working[1] = new Path("/tmp/intmul2"+dateFormat.format(cal.getTime()));
		
		Path outputfolder = new Path(args[1]);
		FileSystem fs = FileSystem.get(clusterConf);
		try {
			/** Copy original matrix to the first input folder*/
			DistCopy.copy(inputfolder, working[0]);
		int current, next;
		do {
			current = iterations%2;
			next = (iterations+1)%2;
			/** Ensure the directory doesn't exist*/
			fs.delete(working[next], true);
			runMultiplication(clusterConf, inputfolder, working[current], working[next]);
			converged = convergence(clusterConf, working[current], working[next]);
			iterations++;
		} while (!converged && maxIterations > iterations);
		if (converged) {
			System.out.println("Reached convergency in "+iterations+" iterations");
			System.out.println(clusterConf.getDouble("convergedDouble", 0.0));
		} else 
			System.out.println("Convergency not reached");
		} catch(IOException io) {
			io.printStackTrace();
			System.exit(-1);
			
		} catch(InterruptedException ie) {
			System.out.println(ie.getMessage()+"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
			ie.printStackTrace();
			System.exit(-1);
		} catch (ClassNotFoundException cnf) {
			cnf.printStackTrace();
			System.exit(-1);
		}
	}
	
	/** Runs the job for matrix multiplication*/
	
	
	private static void runMultiplication(Configuration conf, Path original_data,Path input_path, Path output_path) throws IOException, ClassNotFoundException, InterruptedException {
		 Job job = Job.getInstance(conf, "MatrixMatrixMultiplicationTwoSteps");
	     job.setJarByClass(Driver.class);
	     job.setOutputKeyClass(Text.class);
	     job.setOutputValueClass(Text.class);
	     FileSystem fs = FileSystem.get(conf);
	     job.setReducerClass(FirstStepReduce.class);
	     //job.setInputFormatClass(TextInputFormat.class);
	     job.setOutputFormatClass(TextOutputFormat.class);
	     /** Original data contains the original matrix m0 which is never changed */
	     MultipleInputs.addInputPath(job, original_data, TextInputFormat.class, RowMap.class);
	     /** input path contains the current matrix achieved in the multiplication process */
         MultipleInputs.addInputPath(job, input_path, TextInputFormat.class, ColumnMap.class);
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
	     job2.setReducerClass(SecondStepReducer.class);
	     job2.setInputFormatClass(TextInputFormat.class);
	     job2.setOutputFormatClass(TextOutputFormat.class);
	     FileInputFormat.addInputPath(job2, secondStepOutput);
	     
	     FileOutputFormat.setOutputPath(job2, output_path);
	     job2.submit();
	     if(!job2.waitForCompletion(true)) System.exit(-1);
	     fs.delete(secondStepOutput, true);
	}
	public static enum ConvergenceCounter{
		NOT_CONVERGED;	
	}
	private static boolean convergence(Configuration conf, Path oldMatrix, Path newMatrix) throws IOException, ClassNotFoundException, InterruptedException {
		Job convergence = Job.getInstance(conf, "MatrixConvergenceChecker");
		
        convergence.setJarByClass(Driver.class);
        convergence.setOutputKeyClass(Text.class);
        convergence.setOutputValueClass(DoubleWritable.class);
        convergence.setMapperClass(MarkovConvergenceMapper.class);
        convergence.setReducerClass(MarkovConvergenceReducer.class);
        convergence.setInputFormatClass(TextInputFormat.class);
        convergence.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(convergence, oldMatrix, TextInputFormat.class, MarkovConvergenceMapper.class);
        MultipleInputs.addInputPath(convergence, newMatrix, TextInputFormat.class, MarkovConvergenceMapper.class);
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-s");
		Calendar cal = Calendar.getInstance();
		Path convergenceOutput= new Path("/tmp/convergence"+dateFormat.format(cal.getTime()));
        FileOutputFormat.setOutputPath(convergence, convergenceOutput);
        convergence.submit();
        if (!convergence.waitForCompletion(true)) System.exit(-1);
        FileSystem fs = FileSystem.get(conf);
       
        boolean converged =  convergence.getCounters().findCounter(ConvergenceCounter.NOT_CONVERGED).getValue() <= 0; 
        System.out.println(convergence.getCounters().findCounter(ConvergenceCounter.NOT_CONVERGED).getValue());
        
        if (!converged) System.out.println("Max difference: "+ conf.getDouble("maxDifference", 0.0));
        
        return converged;
   
	}
}
