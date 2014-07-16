package aggregation;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Creates averages for one arc (source dest couple) in one aggregation period.
 *  Takes key AggregationId-SourceId-Dest-id takes as values all the strength for the given arc 
 *  in the aggregation period.
 * */
	public class AverageReducer extends Reducer<Text, DoubleWritable, Text, Text> {
		
        public void reduce(Text key, Iterable<DoubleWritable> v, Context context)
            throws IOException, InterruptedException {
        	// takes ((ID-Num-Source-Dest),(Val))
        	// puts ((ID-Source),(Dest-AvgVal))
        	
        	double w, sum = 0;
        	Iterator<DoubleWritable> values = v.iterator();
        	while(values.hasNext()){
        		w = values.next().get();
        		sum += w;
        	}
        	
        	String[] splitKey = key.toString().split(",");
        	double num = Double.parseDouble(splitKey[1]);
        	
        	double avg = sum / num;
        	
        	Text newKey = new Text(splitKey[0] + "," + splitKey[2]);
        	Text val = new Text(splitKey[3] + "," + avg);
        	context.write(newKey,val);
        	
        }
        
    }