package markov_clustering;
import java.io.IOException;
import java.util.LinkedList;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SumRReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	private MultipleOutputs<Text,DoubleWritable> multipleOutputs;
	 @Override
	 protected void setup(Context context) throws IOException, InterruptedException {
		 multipleOutputs  = new MultipleOutputs<Text, DoubleWritable>(context);
	 }

	public static class Record {
		int blockIdX, blockIdY, rowId, colId;
		double value;
		public Record(int bX, int bY, int r, int c, double v) {
			blockIdX = bX; blockIdY = bY; rowId = r; colId = c; value = v;
		}
		public String getBlockCoordinates() {
			return blockIdX+","+blockIdY;
		}
		public String getLocalCoordinates() {
			return rowId+","+colId;
		}
		public double getValue() { return value;}
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int rowId = Integer.parseInt(key.toString());
		/** Inflation parameter */
		double r = context.getConfiguration().getDouble("inflationParameter", 2);
		double threshold = context.getConfiguration().getDouble("threshold", 0.00001);
		double rescale = 0.0;
		int size = context.getConfiguration().getInt("size", 10000);
		int splits = context.getConfiguration().getInt("splits", 10);
		int split_size = size/splits;
		Double rowPartition_id = Math.floor(rowId/split_size);
		LinkedList<Record> vals = new LinkedList<Record>();
		/** First step: calculate total sum for a single row..! */
		for (Text v : values) {
			/** fields[0] = absolute column identifier, fields[1] is the value*/
			String[] fields = v.toString().split(",");
			double currentValue = Math.pow(Double.parseDouble(fields[1]), r);
			if (currentValue < threshold) continue;
			Double colPartition_id = Math.floor(Integer.parseInt(fields[1])/split_size);
			rescale += currentValue;
			Record representation = new Record(rowPartition_id.intValue(), colPartition_id.intValue(), rowId%split_size, Integer.parseInt(fields[1])%split_size, currentValue);
			vals.add(representation);
		}
			
		for (Record v:vals) {	
			double value = v.getValue();
			value /= rescale;
			multipleOutputs.write(new Text(v.getBlockCoordinates()+"\t"+v.getLocalCoordinates()), new DoubleWritable(value), v.getBlockCoordinates().replace(',', '-')+"/block");
		}
		
	}
	
	@Override
	 protected void cleanup(Context context)
	   throws IOException, InterruptedException {
	  multipleOutputs.close();
	 }
}
