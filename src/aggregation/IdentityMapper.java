package aggregation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IdentityMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException{
		String[] s = v.toString().split("\t");
		Text key = new Text(s[0]);
		Text val = new Text(s[1]);
		c.write(key, val);
	}
}
