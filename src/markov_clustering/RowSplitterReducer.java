package markov_clustering;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class RowSplitterReducer extends Reducer<Text, Text, Text, Text> {
	private MultipleOutputs<Text,Text> multipleOutputs;
	  
	 @Override
	 protected void setup(Context context) throws IOException, InterruptedException {
	  multipleOutputs  = new MultipleOutputs<Text, Text>(context);
	 }
	  
	 @Override
	 protected void reduce(Text key, Iterable<Text> values,Context context)
	   throws IOException, InterruptedException {
	  for(Text value : values) {
	   multipleOutputs.write(key, value, key.toString().replace(',', '-')+"/block");
	  }
	 }
	  
	 @Override
	 protected void cleanup(Context context)
	   throws IOException, InterruptedException {
	  multipleOutputs.close();
	 }
	
}