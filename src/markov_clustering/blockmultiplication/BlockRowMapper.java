package markov_clustering.blockmultiplication;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BlockRowMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split("\t");
		if (Double.parseDouble(fields[2]) == 0) return;
		String[] blockCoordinates = fields[0].split(",");
		Text outK = new Text(blockCoordinates[1]);
		
		Text val = new Text("A,"+fields[1]+","+fields[2]);
		context.write(outK,val);
	}
}
