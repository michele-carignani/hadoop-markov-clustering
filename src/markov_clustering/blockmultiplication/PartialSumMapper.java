package markov_clustering.blockmultiplication;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PartialSumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split("\t");
		context.write(new Text(line[0]), new DoubleWritable(Double.parseDouble(line[1])));
	}
}