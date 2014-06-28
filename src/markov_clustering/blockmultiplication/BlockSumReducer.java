package markov_clustering.blockmultiplication;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BlockSumReducer extends Reducer<Text, DoubleWritable, Text, Text> {
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double sum = 0;
		int blockIdRow = context.getConfiguration().getInt("blockIdRow", 0);
		int blockIdCol = context.getConfiguration().getInt("blockIdCol", 0);
		for(DoubleWritable d: values)
			sum += d.get();
		Text value = new Text(key.toString()+"\t"+sum);
		context.write(new Text(blockIdRow+","+blockIdCol), value);
	}
}