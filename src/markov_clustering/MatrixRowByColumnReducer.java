package markov_clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixRowByColumnReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String[] value;
	    ArrayList<Entry<Integer, Float>> listA = new ArrayList<Entry<Integer, Float>>();
	    ArrayList<Entry<Integer, Float>> listB = new ArrayList<Entry<Integer, Float>>();
	    for (Text val : values) {
	    	value = val.toString().split(",");
	        if (value[0].equals("A")) {
	        	listA.add(new SimpleEntry<Integer, Float>(Integer.parseInt(value[1]), Float.parseFloat(value[2])));
	        } else {
	            listB.add(new SimpleEntry<Integer, Float>(Integer.parseInt(value[1]), Float.parseFloat(value[2])));
	        }
	    }
        String i;
        float a_ij;
        String k;
        float b_jk;
        DoubleWritable outputValue;
        Text out_key;
        for (Entry<Integer, Float> a : listA) {
            i = Integer.toString(a.getKey());
            a_ij = a.getValue();
            if (a_ij != 0){
                for (Entry<Integer, Float> b : listB) {
                    k = Integer.toString(b.getKey());
                    b_jk = b.getValue();
                    if(b_jk != 0){
	                    out_key = new Text(i + "," + k);
	                    outputValue = new DoubleWritable(a_ij * b_jk);
	                    context.write(out_key, outputValue);
                    }
                }
            }
        }
    }
}