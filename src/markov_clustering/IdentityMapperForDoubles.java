package markov_clustering;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IdentityMapperForDoubles extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	public void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException{
		String[] content = v.toString().split("\t");
		c.write(new Text(content[0]), new DoubleWritable(Double.parseDouble(content[1])));
		}
}