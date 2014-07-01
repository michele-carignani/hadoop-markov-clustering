package markov_clustering;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Counts the differences between values in the old and in the current matrix 
 * to check for convergence.
 *
 */
public class ConvergenceReducer extends Reducer<Text,DoubleWritable,Text,Text> {
	
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
			context.getCounter(Convergence.ConvergenceCounter.NOT_CONVERGED).increment(1);
		} 
		context.write(coordinates, new Text(prev+"\t"+next)); //For debugging purposes, remember to remove
	}
}