package markov_clustering;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import markov_clustering.blockmultiplication.BlockMultiplier;
import markov_clustering.blockmultiplication.BlockWiseMatrixMultiplication;
import markov_clustering.test.StochasticRowVerifier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;


public class Driver {
    
	
	/**
	 * @param args[0] input folder
	 * @param args[1] output folder
	 * @param arg[2] maximum number of iterations
	 * @param arg[3] number of workers (not mandatory; if not specified will be one for each row)
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		long beginning = System.nanoTime();
		Configuration clusterConf = new Configuration();
		
		int iterations = 0;
		int maxIterations = Integer.parseInt(args[2].trim());
		int current, next, prev, inflate = 3;
		
		Path[] working = new Path[4];
		
		boolean converged = false;
		
		clusterConf.setDouble("inflationParameter", 4);
		/** Directory for the original matrix M0 */
		Path inputfolder = new Path(args[0]);
		clusterConf.setDouble("threshold", 0.00001);
		clusterConf.setBoolean("converged", true);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
	    Calendar cal = Calendar.getInstance();
		String[] dirs= new String[]{
				"/tmp/A-Partitioned-"+dateFormat.format(cal.getTime()), 
				"/tmp/B-Partitioned-"+dateFormat.format(cal.getTime()), 
				"/tmp/C-Partitioned-"+dateFormat.format(cal.getTime())
		};
		try {
			ToolRunner.run(clusterConf, new MatrixSplitter(), new String[]{args[0], dirs[0]});
			ToolRunner.run(clusterConf, new MatrixSplitter(), new String[]{args[0], dirs[1]});
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(-1);
		/** Working directory 1, at first step initialize with copy of original matrix M0 */
		working[0] = new Path(dirs[0]);
		working[1] = new Path(dirs[1]);
		working[2] = new Path(dirs[2]);		
		working[inflate] = new Path("/tmp/Inflate-"+dateFormat.format(cal.getTime()));
		FileSystem fs = FileSystem.get(clusterConf);
		
		
		try {

			
			do {
				System.out.println(
				"##################################################################################"
				);
				System.out.println(
				"#                    Iteration n.    "+iterations+"                                            #"
				);
				System.out.println(
						"##################################################################################"
						);
				prev = iterations%3;
				current = (iterations+1)%3;
				next = (iterations+2)%3;
				
				/** Ensure the directories do not exist*/
				fs.delete(working[next], true);
				//fs.delete(working[inflate], true);
				
				/** Run a two step matrix multiplication map-reduce job */
				ToolRunner.run(clusterConf, new BlockWiseMatrixMultiplication(), new String[]{dirs[prev], dirs[current], dirs[next]});
				System.exit(-1);
				/** Inflation, for making convergence faster. Default r is = 2 
				 * */	
				Inflation.run(clusterConf, working[inflate], working[next]);
				
				/** Run a one step map-reduce job to check convergece. Default threshold is 10^-5 */
				converged = Convergence.run(clusterConf, working[current], working[next]);
				
				iterations++;
				
			} while (!converged && maxIterations > iterations);
			if (converged) {
				System.out.println("Reached convergency in "+iterations+" iterations");
				System.out.println(clusterConf.getDouble("convergedDouble", 0.0));
				
			} else { 
				System.out.println("Convergency not reached");
			}
			System.out.println("Running correctness diagnosis:");
			StochasticRowVerifier.run(clusterConf, working[next], new Path("/tmp/stochasticVerifier"+dateFormat.format(cal.getTime())));
			
		} catch(IOException io) {
			
			io.printStackTrace();
			System.exit(-1);
			
		} catch(InterruptedException ie) {
			
			System.out.println(ie.getMessage());
			ie.printStackTrace();
			System.exit(-1);
			
		} catch (ClassNotFoundException cnf) {
			cnf.printStackTrace();
			System.exit(-1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long end = System.nanoTime();
		System.out.println((end-beginning)/1000 + " microseconds of execution time");
	}
	
	/** Runs the job for matrix multiplication*/
}
