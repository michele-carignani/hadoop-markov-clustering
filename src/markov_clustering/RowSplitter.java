package markov_clustering;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RowSplitter extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	/**
	 * Splits into separate partitions
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		int size = context.getConfiguration().getInt("size", 10000);
		int splits = context.getConfiguration().getInt("splits", 10);
		int split_size = size/splits;
		String[] fields = value.toString().split("\t");
		String[] coordinates = fields[0].split(",");
		Double rowPartition_id = Math.floor(Integer.parseInt(coordinates[0]) / split_size);
		Double colPartition_id = Math.floor(Integer.parseInt(coordinates[1])/split_size);
		Text outKey = new Text(rowPartition_id.intValue()+","+colPartition_id.intValue());
		Text outVal = new Text(Integer.parseInt(coordinates[0])%split_size+","+Integer.parseInt(coordinates[1])%split_size+"\t"+fields[1]);
		context.write(outKey, outVal);
	}
}