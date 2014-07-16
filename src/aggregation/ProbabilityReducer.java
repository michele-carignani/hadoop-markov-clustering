package aggregation;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;



public class ProbabilityReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	
	private MultipleOutputs<Text,DoubleWritable> multipleOutputs;
	 @Override
	 protected void setup(Context context) throws IOException, InterruptedException {
		 multipleOutputs  = new MultipleOutputs<Text, DoubleWritable>(context);
	 }
	 @Override
	 protected void cleanup(Context context)
			   throws IOException, InterruptedException {
			  multipleOutputs.close();
	 }
	
	
	private class ArcTail implements Comparable<ArcTail>{
		public ArcTail(String destination, double weight) {
			this.destination = destination;
			this.weight = weight;
		}
		String destination; double weight;
		@Override
		public int compareTo(ArcTail arc) {
			return this.destination.compareTo(arc.destination);
		}
	}
	
	
	
	public void reduce(Text aggregation, Iterable<Text> v, Context context)
	            throws IOException, InterruptedException {
		/** params
		 * 0 -> aggregationName
		 * 1 -> source
		 */
		/** value
		 *  0 -> destination
		 *  1 -> sum
		 */
		String[] params = aggregation.toString().split(","), vSplit;
		double sum = 0; 
		
		List<ArcTail> al = new LinkedList<ArcTail>();
		Iterator<Text> values = v.iterator();
		while(values.hasNext()) {
			vSplit = values.next().toString().split(",");
			double w = Double.parseDouble(vSplit[1]);
			al.add(new ArcTail(vSplit[0], w));
			sum += w;
		}
		if (sum == 0) return;
		Collections.sort(al);
		String previousDest = "";
		double previousValue = 0;
		for(ArcTail a : al){
			if (previousDest.equals(a.destination)) {
				previousValue += a.weight;
				continue;
			}
			//differs and is not null, thus the previous has to be written in output
			if (previousValue > 0) {
				Text newKey = new Text(params[1]+","+previousDest);
				multipleOutputs.write(newKey, new DoubleWritable(previousValue/sum), params[0]+"/matrix");
			}
			//initialize for the new value..
			previousDest = a.destination;
			previousValue = a.weight;
		}
		if (previousValue > 0) {
			Text newKey = new Text(params[1]+","+previousDest);
			multipleOutputs.write(newKey, new DoubleWritable(previousValue/sum), params[0]+"/matrix");
		}
			
		al.clear();
		al = null;
		
		
	}

}
