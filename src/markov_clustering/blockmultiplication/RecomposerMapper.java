package markov_clustering.blockmultiplication;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecomposerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//Input format is blockIdX, blockIdY \t localRowId,localColId \t value
		String[] fields = value.toString().split("\t");
		String[] blockCoordinates = fields[0].split(",");
		String[] localCoordinates = fields[1].split(",");
		double probability = Double.parseDouble(fields[2]);
		int size = context.getConfiguration().getInt("size", 10000);
		int splits = context.getConfiguration().getInt("splits", 10);
		int split_size = size/splits;
		int row = Integer.parseInt(localCoordinates[0])+Integer.parseInt(blockCoordinates[0])*split_size;
		int column = Integer.parseInt(localCoordinates[1])+Integer.parseInt(blockCoordinates[1])*split_size;
		context.write(new Text(row+","+column), new DoubleWritable(probability));
	}
}
