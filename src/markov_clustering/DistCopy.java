package markov_clustering;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DistCopy {
    
    public static class CopyMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException{
        	String[] values = v.toString().split("\t");
        	Text key = new Text(values[0]);
            c.write(key, new Text(values[1]));
        }
    }
    
    public static class CopyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            for(Text v : values){
                context.write(key, v);
            }
            
        }
    }
    
    public static boolean copy(Path from , Path to) 
            throws IOException, ClassNotFoundException, InterruptedException{
        
        Configuration c = new Configuration();
        
        Job j = Job.getInstance(c, "Distributed copy from " + from + " to " + to);

        j.setJarByClass(DistCopy.class);
        
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
 
        j.setMapperClass(CopyMapper.class);
        j.setReducerClass(CopyReducer.class);
        
        j.setInputFormatClass(TextInputFormat.class);
	    j.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(j, from);
        FileOutputFormat.setOutputPath(j, to);
        
        j.submit();
        return j.waitForCompletion(true);
    }
}
