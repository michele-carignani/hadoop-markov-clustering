package markov_clustering;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/** Receives in input a series of blocks with rows in format blockIdX,blockIdY \t rowId,colId, value*/
public class MatrixRowMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        String[] blockCoordinates = line[0].split(",");
        String[] relativeCoordinates = line[1].split(",");
        int size = context.getConfiguration().getInt("size", 10000);
		int splits = context.getConfiguration().getInt("splits", 10);
		int split_size = size/splits;
        int absoluteRow = Integer.parseInt(relativeCoordinates[0])+Integer.parseInt(blockCoordinates[0])*split_size;
        int absoluteCol = Integer.parseInt(relativeCoordinates[1])+Integer.parseInt(blockCoordinates[1])*split_size;
        Text outputKey = new Text(Integer.toString(absoluteRow));
        //Column coordinate, value.
        Text outputValue = new Text(absoluteCol+","+line[2]);	            
        context.write(outputKey, outputValue); 
    }
    /** Returns A (meaning: row),absoluteRow,absoluteColumn value*/
}
