package markov_clustering.blockmultiplication;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 
 * Input format: blockIdX,blockIdY \t localRowId,LocalColId \t Value
 *
 */
public class MatrixRowMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        String[] indicesAndValue = line[1].split(",");
        Text outputKey = new Text();
        Text outputValue = new Text();	            
            outputKey.set(indicesAndValue[0]);
            outputValue.set("A," + indicesAndValue[1] + "," + line[2]);
            context.write(outputKey, outputValue);
    }
}
