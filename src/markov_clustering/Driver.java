package markov_clustering;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class Driver {
    
	
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
		int current, next, inflate = 2;
		
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
		
		working[inflate] = new Path("/tmp/inflate"+dateFormat.format(cal.getTime()));
		
		FileSystem fs = FileSystem.get(clusterConf);
		
		
		try {
			/** Copy original matrix to the first input folder*/
			DistCopy.copy(inputfolder, working[0]);
			
			do {
				current = iterations%2;
				next = (iterations+1)%2;
				
				/** Ensure the directory doesn't exist*/
				fs.delete(working[next], true);
				
				/** Run a two step matrix multiplication map-reduce job */
				MatrixMultiplication.run(clusterConf, inputfolder, working[current], working[inflate]);
				
				Inflation.run(working[inflate], working[next]);
				
				/** Run a one step map-reduce job to check convergece */
				converged = Convergence.run(clusterConf, working[current], working[next]);
				
				iterations++;
				
			} while (!converged && maxIterations > iterations);
			if (converged) {
				
				System.out.println("Reached convergency in "+iterations+" iterations");
				System.out.println(clusterConf.getDouble("convergedDouble", 0.0));
				
			} else { 
				
				System.out.println("Convergency not reached");
				
			}
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
		}
	}
	
	/** Runs the job for matrix multiplication*/
}
