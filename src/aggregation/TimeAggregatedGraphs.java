package aggregation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/** Generate aggregated sums of strengths over the nodes
 *  for each hour interval
 */

public class TimeAggregatedGraphs {
	
	
	public static Job filterJob(Configuration conf, int currentChunk, int numReducers, String tmpDir) throws IOException {
		Job job = Job.getInstance(conf, "Time Aggregated Graphs");
    	job.setJarByClass(TimeAggregatedGraphs.class);
    	job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    job.setCombinerClass(FilterCombiner.class);
	    job.setReducerClass(AverageReducer.class);
	    job.setMapperClass(FilterMapper.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
	    job.setInputFormatClass(TextInputFormat.class);
        Path intermediate =  new Path(tmpDir+"/"+currentChunk+"/");
		FileOutputFormat.setOutputPath(job, intermediate);
		job.setNumReduceTasks(numReducers);
		return job;
	}
	/**
	 * @param args[0] aggregation file
	 * @param args[1] input directory
	 * @param args[2] output directory
	 * @param args[3] size of chunks to be processed together, put 0 for processing all of it together.
	 * @param args[4] number of reducers
	 * @throws Exception
	 */
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
        	System.out.println("Usage: call with aggregationFile (local, not hdfs) inputDirectory outputDirectory");
        	System.exit(-1);
        }
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        /** Read the list of aggregators from the file passed as first arg.
         *  The file must be on hdfs*/
        String aggregators = "";
        int countAggregators = 0;
        try {
        	Path aggregationFile = new Path(args[0]);
        	
        	BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(aggregationFile)));
        	String agg = "";
        	agg = reader.readLine();
        	while(agg != null) {
        		aggregators += agg+"\n";
        		countAggregators++;
        		agg = reader.readLine();
        	}
        } catch(IOException io) {
        	io.printStackTrace();
        	System.exit(-1);
        }
    	if(countAggregators == 0) {
    		System.out.println("Please provide a valid aggregation file");
    		System.exit(-1);
    	}
    	System.out.println("Aggregators:"+countAggregators);
    	System.out.println(aggregators);
    	conf.set("globalAggregators", aggregators);
    	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
        Calendar cal = Calendar.getInstance();
    	String tmpDir = "/tmp/intermediate_filter"+dateFormat.format(cal.getTime());
    	int chunkSize = (args.length > 3) ? Integer.parseInt(args[3]) : 1;
    	int numReducers = (args.length > 4) ? Integer.parseInt(args[4]) : countAggregators;
    	int currentChunkSize = 0;
    	int currentChunk = 0;
    	Job job = filterJob(conf, currentChunk, numReducers, tmpDir);
	    /** Divide the files in jobs and calculate them one by one to avoid issues.*/
    	for (FileStatus file : fs.listStatus(new Path(args[1]))) {
    		 if(file.isFile())
    			 FileInputFormat.addInputPath(job, file.getPath());
    		 currentChunkSize++;
    		 if (currentChunkSize == chunkSize) { //Submit the job and wait for it to return.
    			 currentChunkSize = 0;
    			 currentChunk++;
    			 job.submit();
    			 if(!job.waitForCompletion(true)) System.exit(-1);
    			 job = filterJob(conf, currentChunk, numReducers, tmpDir);
    		 }
    	}
    	/** Filter remaining files */
	    if (currentChunkSize > 0) {
	    	job.submit();
			if(!job.waitForCompletion(true)) System.exit(-1);
	    }
	    
        Job second = Job.getInstance(conf, "create aggregated probability graphs");
        second.setJarByClass(TimeAggregatedGraphs.class);
        
        second.setMapperClass(IdentityMapper.class);
        LazyOutputFormat.setOutputFormatClass(second, TextOutputFormat.class);
        second.setReducerClass(ProbabilityReducer.class);
        FileInputFormat.addInputPath(second, new Path(tmpDir+"/*/"));
        second.setOutputKeyClass(Text.class);
        second.setOutputValueClass(Text.class);
        Path output = new Path(args[2]);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(second, output);
        second.setInputFormatClass(TextInputFormat.class);
        second.submit();
        boolean success = second.waitForCompletion(true);
        fs.delete(new Path(tmpDir), true);
        System.exit((success) ? 0 : 1);
    }

}
