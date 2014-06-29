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
public class MatrixColumnMapper extends Mapper<LongWritable, Text, Text, Text> {
	
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	/**
    	 * Initial format is i,j \t strength
    	 */
        String[] line = value.toString().split("\t");
        String[] indicesAndValue = line[1].split(",");
        String[] blockCoordinates = line[0].split(",");
        Text outputKey = new Text();
        Text outputValue = new Text();
        
       	outputKey.set(blockCoordinates[0]+","+indicesAndValue[1]);
       	/** B flag is used to represent the second matrix */
        outputValue.set("B," + indicesAndValue[0] + "," + line[2]+","+blockCoordinates[1]);
        
        context.write(outputKey, outputValue);
    }
    
}