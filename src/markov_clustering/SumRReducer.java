package markov_clustering;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumRReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	
	private static final Log LOG = LogFactory.getLog(SumRReducer.class);
	
	/** Comparator for sorting the values; not used currently. Will be used to remove the smallest elements in a row  */
	private static Comparator<Entry<Text, Double>> EntryComparator = new Comparator<Entry<Text, Double>>() {

		public int compare(Entry<Text, Double> o1, Entry<Text, Double> o2) {
			return Double.compare(o1.getValue(), o2.getValue());
		}
	};
	
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String columnId = key.toString();
		/** Inflation parameter */
		double r = context.getConfiguration().getDouble("inflationParameter", 2);
		
		double rescale = 0.0;
		LinkedList<Entry<Text, Double>> vals = new LinkedList<Entry<Text, Double>>();
		for (Text v : values) {
			/** fields[0] == 'B', not used here, fields[1] is the row, fields[2] is the value*/
			String[] fields = v.toString().split(",");
			double currentValue = Double.parseDouble(fields[2]);
			Text coordinates = new Text(fields[1]+","+columnId);
			rescale += Math.pow(Double.parseDouble(fields[2]), r);
			vals.add(new SimpleEntry<Text, Double>(coordinates, currentValue));
		}
		
		
		for (Entry<Text, Double> v:vals) {	
			double value = Math.pow(v.getValue(), r);
			value /= rescale;
			context.write(v.getKey(), new DoubleWritable(value));
			LOG.debug("==> Write value: " + value + " fields:");
		}
		
	}
}
