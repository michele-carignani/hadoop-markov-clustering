package markov_clustering;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Emits values keyed by their coordinates. One mapper is launched for the old and another for the new matrix.
 */
public class ConvergenceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
/** Format of the incoming file is row,column\t value */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] s = value.toString().split("\t");
			Text outKey = new Text(s[0]);
			DoubleWritable outVal = new DoubleWritable(Double.parseDouble(s[1]));
			context.write(outKey, outVal);
		
	}

}