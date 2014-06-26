package markov_clustering.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CheckStochasticReducer extends Reducer<Text, Text, IntWritable, Text> {
	public void reducer(Text rowId, Iterable<Text> rowValues, Context context) throws IOException, InterruptedException {
		double total = 0.0;
		for(Text v : rowValues) {
			String[] fields = v.toString().split(",");
			total += Double.parseDouble(fields[2]);
		}
		if (total != 1) context.write(new IntWritable(Integer.parseInt(rowId.toString())), new Text("NOT STOCHASTIC: "+total));
	}
}
