package markov_clustering;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixColumnMapper extends Mapper<LongWritable, Text, Text, Text> {
	
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
        String[] line = value.toString().split("\t");
        String[] indicesAndValue = line[0].split(",");
        
        Text outputKey = new Text();
        Text outputValue = new Text();
        
       	outputKey.set(indicesAndValue[1]);
        outputValue.set("B," + indicesAndValue[0] + "," + line[1]);
        
        context.write(outputKey, outputValue);
    }
    
}