package markov_clustering;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumRReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	
	private static final Log LOG = LogFactory.getLog(SumRReducer.class);
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String columnId = key.toString();
		/** Inflation parameter */
		double r = context.getConfiguration().getDouble("inflationParameter", 2);
		double rescale = 0.0;
		for (Text v : values) {
			/** fields[0] == 'B', not used here, fields[1] is the row, fields[2] is the value*/
			String[] fields = v.toString().split(",");
			rescale += Math.pow(Double.parseDouble(fields[2]), r);
		}
		for (Text v:values) {
			String[] fields = v.toString().split(",");
			double value = Math.pow(Double.parseDouble(fields[2]), r);
			value /= rescale;
			context.write(new Text(fields[1]+","+columnId), new DoubleWritable(value));
			LOG.info("==> Write value: " + value + " fields: " + fields[1]+","+columnId);
		}
	}
}
