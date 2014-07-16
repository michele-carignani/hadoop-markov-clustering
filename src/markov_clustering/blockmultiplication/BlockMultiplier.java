package markov_clustering.blockmultiplication;

import java.io.IOException;
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
 * Prepares and performs multiplications of matrix blocks. 
 */
public class BlockMultiplier extends Configured implements Tool {
	/** Current block identifier in term of row index and column index */
	/** Polling interval to check if the thread for managing the block multiplication has finished*/
	private int sleepTimeMillis = 3000;
	/** List of directory to clean-up on exit*/
	private LinkedList<Path> tmpmul = new LinkedList<Path>();
	/** Set of blocks to be calculated by a certain instance */
	private MatrixBlocks partition;
	/** Directories */
	private String firstMatrix;
	private String secondMatrix;
	private String resultDirectory;
	
	/** Number of reducers */
	private int numReducers;
	
	/**
	 * Prepares for the execution of a single block multiplication
	 * @param blockIdX the x-coordinate of the block to multiply
	 * @param blockIdY the y-coordinate of the block to multiply
	 * @param firstMatrix the directory in which the blocks of the first matrix are placed. Blocks are assumed to be memorized inside here (also in separate files) within folders with name /blockidx-blockidy
	 * @param secondMatrix the directory in which the blocks of the second matrix are placed. See for first-column the convention about format.
	 * @param resultDirectory the directory in which the resulting block will be written
	 */
	public BlockMultiplier(int blockIdX, int blockIdY, String firstMatrix, String secondMatrix, String resultDirectory, int reducers) {
		super();
		partition = new MatrixBlocks(blockIdX, blockIdY);
		initializeDirectories(firstMatrix, secondMatrix, resultDirectory);
		numReducers = Math.max(1,reducers);
	}
	
	/**
	 * Prepares for the execution of a single block multiplication
	 * @param blocks MatrixBlocks object containing the list of blocks for which the multiplication has to be executed.
	 * @param firstMatrix the directory in which the blocks of the first matrix are placed. Blocks are assumed to be memorized inside here (also in separate files) within folders with name /blockidx-blockidy
	 * @param secondMatrix the directory in which the blocks of the second matrix are placed. See for first-column the convention about format.
	 * @param resultDirectory the directory in which the resulting block will be written
	 */
	public BlockMultiplier(MatrixBlocks blocks, String firstMatrix, String secondMatrix, String resultDirectory, int reducers) {
		super();
		partition = blocks;
		initializeDirectories(firstMatrix, secondMatrix, resultDirectory);
		numReducers = Math.max(1,reducers);
	}
	
	private void initializeDirectories(String input1, String input2, String output) {
		
		firstMatrix = input1; secondMatrix = input2; resultDirectory = output;
	}
	
	@Override
	/**
	 *  Runs the multiplication job(s) for the selected matrix partition
	 *  @returns exit code (0 success, -1 failure);
	 */
	public int run(String[] arg) throws IOException, InterruptedException  {
		Configuration conf = super.getConf();
		JobControl multiplicationControllerStub = new JobControl("BlockWise Multiplication");
		for(Block block: partition)
			addSubMultiplication2(conf, multiplicationControllerStub, block);
		/** JobControl in a separate thread*/
		final JobControl multiplicationController = multiplicationControllerStub;
		final Thread multiplicationControllerExecutor = new Thread(){
			@Override
			public void run() {
				multiplicationController.run();
			}
		};
		/** Polling of the job controller */
		multiplicationControllerExecutor.start();
		while(!multiplicationController.allFinished()) {
			Thread.sleep(sleepTimeMillis);
		}
		//Stops the  JobControl
		multiplicationController.stop();
		//Waits the other thread to join
		multiplicationControllerExecutor.join(0);
		FileSystem fs = FileSystem.get(conf);
		clean(fs);	
		return (multiplicationController.getFailedJobList().isEmpty()) ? 0 : -1;
	}
	
	
	private void clean(FileSystem fs) {
		try {
		for(Path p: tmpmul)		fs.delete(p, true);
		} catch (IOException e) {
			System.out.println("Couldn't clean the temporary directory. Please do it by hands:");
			for (Path p: tmpmul) System.out.println(p.toString());
			e.printStackTrace();
			System.exit(-1);
		}
	}
	/*
	public static class BrutalRowMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split("\t");
			String[] blockCoordinates = fields[0].split(",");
			Text outK = new Text(blockCoordinates[1]);
			Text val = new Text("A,"+fields[1]+","+fields[2]);
			context.write(outK,val);
		}
	}
	public static class BrutalColumnMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split("\t");
			String[] blockCoordinates = fields[0].split(",");
			Text outK = new Text(blockCoordinates[0]);
			Text val = new Text("B,"+fields[1]+","+fields[2]);
			context.write(outK, val);
		}
	}
	
	public static class BrutalReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int totSize = context.getConfiguration().getInt("size", 10000);
			int splits = context.getConfiguration().getInt("splits", 10);
			int matrixSize = totSize/splits;
			double[][] matrix_A = new double[matrixSize][matrixSize];
			double[][] matrix_B = new double[matrixSize][matrixSize];
			//Memorize all values...
			for(Text v: values) {
				String[] fields = v.toString().split(",");
				int x = Integer.parseInt(fields[1]);
				int y = Integer.parseInt(fields[2]);
				double value = Double.parseDouble(fields[3]);
				if (fields[0].equals("A"))
					matrix_A[x][y] = value;
				else
					matrix_B[x][y] = value;
			}
			
			for (int i = 0; i < matrixSize; i++) {
				for(int j = 0; j < matrixSize; j++) {
					double sum = 0;
					for (int k = 0; k < matrixSize; k++) {
						sum += matrix_A[i][k]*matrix_B[k][j];
					}
					context.write(new Text(i+","+j), new DoubleWritable(sum));
				}
			}
			
		}
	}
	*/
	private void addSubMultiplication2(Configuration conf, JobControl controller, Block block) throws IOException {
		/** First job configuration */
		ControlledJob couplesMultiplication = new ControlledJob(conf);
		Job job = couplesMultiplication.getJob();
		job.setJobName("Block Matrix multiplication");
		job.setOutputKeyClass(Text.class);
		job.setReducerClass(MatrixMultiplicationReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	    job.setJarByClass(Driver.class);
		MultipleInputs.addInputPath(job, new Path(firstMatrix+"/"+block.getRow()+"-*"), TextInputFormat.class, BlockRowMapper.class);
		MultipleInputs.addInputPath(job, new Path(secondMatrix+"/*-"+block.getColumn()), TextInputFormat.class, BlockColumnMapper.class);
		Path sumInputPath = new Path(resultDirectory+"/tmpsumAB-"+block.getRow()+"-"+block.getColumn());
		FileOutputFormat.setOutputPath(job, sumInputPath);
		job.setNumReduceTasks(numReducers);
		tmpmul.add(sumInputPath);
		
		/** Second job configuration */
		
		/** Tell the second job which matrix is now handled */
		conf.setInt("blockIdRow", block.getRow());
		conf.setInt("blockIdCol", block.getColumn());
		ControlledJob totalSum = new ControlledJob(conf);
		Job sumJob = totalSum.getJob();
		sumJob.setJobName("Sum of partial matrix products");
		sumJob.setOutputKeyClass(Text.class);
		sumJob.setOutputValueClass(DoubleWritable.class);
		sumJob.setInputFormatClass(TextInputFormat.class);
		sumJob.setOutputFormatClass(TextOutputFormat.class);
		sumJob.setMapperClass(PartialSumMapper.class);
		sumJob.setReducerClass(BlockSumReducer.class);
		sumJob.setJarByClass(Driver.class);
		sumJob.setNumReduceTasks(numReducers);
		FileInputFormat.addInputPath(sumJob, sumInputPath);
		FileOutputFormat.setOutputPath(sumJob, new Path(resultDirectory+"/"+block.getRow()+"-"+block.getColumn()+"/"));
		totalSum.addDependingJob(couplesMultiplication);
		/** Add to the controller */
		controller.addJob(couplesMultiplication); controller.addJob(totalSum);
	}
	
	/** Adds a multiplication task to the controller controller 
	 * @param conf the current configuration
	 * @param controller the controller to which the jobs have to be added
	 * @param inputDir1 the directory of the first matrix
	 * @param inputDir2 the directory of the second matrix
	 * @param outputDir the destination directory
	 * @throws IOException 

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
	     TextOutputFormat.setCompressOutput(innerJob, true);
	     TextOutputFormat.setOutputCompressorClass(innerJob, GzipCodec.class);
	     innerJob.setJarByClass(Driver.class);
	     
	     /** Input from partition of matrix 1 
	     MultipleInputs.addInputPath(innerJob, new Path(inputDir1+"/"+rowId+"-*"), TextInputFormat.class, MatrixRowMapper.class);
	     /** input from partition of matrix 2 
	     MultipleInputs.addInputPath(innerJob, new Path(inputDir2+"/*-"+colId), TextInputFormat.class, MatrixColumnMapper.class);
	     /** Initialization of temporary dir 
	     DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss"+Math.random());
		 Calendar cal = Calendar.getInstance();
		 Path secondStepOutput= new Path("/tmp/mul2tmp"+dateFormat.format(cal.getTime())+"-"+Math.random()+"-"+System.nanoTime());
	     /**Add the path to the list of temporary folders to be cleaned
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
		 innerJob.setNumReduceTasks(5);
		 innerJob2.setNumReduceTasks(5);
		 sumJob.setNumReduceTasks(5);
		 FileInputFormat.addInputPath(sumJob, sumInputPath);
		 FileOutputFormat.setOutputPath(sumJob, new Path(outputDir+"/"+rowId+"-"+colId+"/"));	     
	     controller.addJob(job); controller.addJob(job2); controller.addJob(controlledSumJob);

	}
*/
}