package markov_clustering;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

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
			conf.setBoolean("converged", false);
			conf.setDouble("convergedValue", difference);
		} else {
			double maxdiff = conf.getDouble("maxDifference", 0.0);
			conf.setDouble("maxDifference", Math.max(difference, maxdiff));
		}
	
		context.write(coordinates, new Text(prev+"\t"+next));
	}
}