package aggregation;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		//Since they will count the same, if they have the same values we will return their average.
		double sum = 0;
		for (DoubleWritable d: values) {
			sum += d.get();
		}
		context.write(key, new DoubleWritable(sum));
	}

}
