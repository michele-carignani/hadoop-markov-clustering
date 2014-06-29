package markov_clustering.blockmultiplication;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedList;

import markov_clustering.Driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
/**
 * Cares for all the blocks multiplication regarding a certain block of the final matrix.
 */
public class BlockMultiplier extends Configured implements Tool {

	
	/**
	 * @param arg[0] input dir 1
	 * @param arg[1] input dir 2
	 * @param arg[2] destination directory
	 * @param arg[3] block row id
	 * @param arg[4] block column id
	 * 
	 */
	private int rowId = 0;
	private int colId = 0;
	private int sleepTimeMillis = 3000;
	private LinkedList<Path> tmpmul = new LinkedList<Path>();
		
	@Override
	public int run(String[] arg) throws Exception {
				Configuration conf = super.getConf();
		rowId = Integer.parseInt(arg[3]);
		colId = Integer.parseInt(arg[4]);
		FileSystem fs = FileSystem.get(conf);
			final JobControl multiplicationController = new JobControl("Multiplications for Block "+arg[3]+","+arg[4]);

			addSubMultiplication(conf, multiplicationController, arg[0], arg[1], arg[2]);
			
		Thread multiplicationControllerExecutor = new Thread(){
			@Override
			public void run() {
				multiplicationController.run();
			}
		};
			
			/*Start the job control in a separate thread*/
			multiplicationControllerExecutor.start();
			while(!multiplicationController.allFinished()) {
				Thread.sleep(sleepTimeMillis);
			}
			multiplicationController.stop(); //Make the other thread return
			multiplicationControllerExecutor.join(0);
			for(Path p: tmpmul) { //delete temporary files used between Ist and IInd multiplication step
				 fs.delete(p, true);
			}
			tmpmul.clear();
			if (!multiplicationController.getFailedJobList().isEmpty()) System.exit(-1);
			return 0;
	}
	

	/** Adds a multiplication task to the controller controller 
	 * @param conf the current configuration
	 * @param controller the controller to which the jobs have to be added
	 * @param inputDir1 the directory of the first matrix
	 * @param inputDir2 the directory of the second matrix
	 * @param outputDir the destination directory
	 * @throws IOException */
	
	private void addSubMultiplication(Configuration conf,
			JobControl controller, String inputDir1, String inputDir2,
			String outputDir) throws IOException {
		ControlledJob job = new ControlledJob(conf);
		 job.setJobName("Matrix multiplication Ist step");
		 Job innerJob = job.getJob();
	     //TODO: check if needed innerJob.setJarByClass(BlockMultiplication.class);
	     innerJob.setOutputKeyClass(Text.class);
	     innerJob.setOutputValueClass(Text.class);
	     innerJob.setReducerClass(MatrixRowByColumnReducer.class);
	     innerJob.setOutputFormatClass(TextOutputFormat.class);
	     innerJob.setJarByClass(Driver.class);
	     /** Input from partition of matrix 1 */
	     MultipleInputs.addInputPath(innerJob, new Path(inputDir1+"/"+rowId+"-*"), TextInputFormat.class, MatrixRowMapper.class);
	     /** input from partition of matrix 2 */
	     MultipleInputs.addInputPath(innerJob, new Path(inputDir2+"/*-"+colId), TextInputFormat.class, MatrixColumnMapper.class);
	     /** Initialization of temporary dir */
	     DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss"+Math.random());
		 Calendar cal = Calendar.getInstance();
		 Path secondStepOutput= new Path("/tmp/mul2tmp"+dateFormat.format(cal.getTime())+"-"+Math.random()+"-"+System.nanoTime());
	     /**Add the path to the list of temporary folders to be cleaned*/
		 tmpmul.add(secondStepOutput);
		 FileOutputFormat.setOutputPath(innerJob, secondStepOutput);
	     ControlledJob job2 = new ControlledJob(conf);
	     job2.setJobName("Matrix multiplication IInd step");
	     job2.addDependingJob(job);
	     Job innerJob2 = job2.getJob();  
	     innerJob2.setOutputKeyClass(Text.class);
	     innerJob2.setOutputValueClass(DoubleWritable.class);
	     innerJob2.setJarByClass(Driver.class);
	     innerJob2.setMapperClass(IdentityMapperForDoubles.class);
	     innerJob2.setReducerClass(MatrixSumReducer.class);
	     innerJob2.setInputFormatClass(TextInputFormat.class);
	     innerJob2.setOutputFormatClass(TextOutputFormat.class);
	     FileInputFormat.addInputPath(innerJob2, secondStepOutput);
	     Path sumInputPath = new Path(outputDir+"/tmpsumAB-"+rowId+"-"+colId);
	     tmpmul.add(sumInputPath);
	     FileOutputFormat.setOutputPath(innerJob2, sumInputPath);
	     innerJob2.setSpeculativeExecution(true);
	     innerJob.setSpeculativeExecution(true);
		 conf.setInt("blockIdRow", rowId);
		 conf.setInt("blockIdCol", colId);
		 ControlledJob controlledSumJob = new ControlledJob(conf);
		 controlledSumJob.setJobName("Matrix Multiplication - Sum of partial products");
		 controlledSumJob.addDependingJob(job2);
		 Job sumJob = controlledSumJob.getJob();
		 sumJob.setOutputKeyClass(Text.class);
		 sumJob.setOutputValueClass(DoubleWritable.class);
		 sumJob.setInputFormatClass(TextInputFormat.class);
		 sumJob.setOutputFormatClass(TextOutputFormat.class);
		 sumJob.setMapperClass(PartialSumMapper.class);
		 sumJob.setReducerClass(BlockSumReducer.class);
		 sumJob.setJarByClass(Driver.class);
		 FileInputFormat.addInputPath(sumJob, sumInputPath);
		 FileOutputFormat.setOutputPath(sumJob, new Path(outputDir+"/"+rowId+"-"+colId+"/"));	     
	     controller.addJob(job); controller.addJob(job2); controller.addJob(controlledSumJob);
	     
	}

}
