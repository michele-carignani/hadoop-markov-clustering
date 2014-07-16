package aggregation;
import java.io.IOException;
import java.util.Calendar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * FilterMapper reads a rough record with format
 * timestamp \t SourceId \t DestId \t Strength
 * then performs a filtering using the aggregators memorized in the context and writes all the records 
 * respecting one aggregator as
 * AggregationId,PeriodDuration,SourceId,DestId , Strength
 */

public class FilterMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	@Override			
	public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
			
		// Parse the line, format is Timestamp \t SourceID \t DestID \t Strength 
		String[] record = line.toString().split("\t");
		
		Long timestamp = Long.parseLong(record[0]);
		
		// Find the time at the start of the day
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(timestamp);
		
		int d = cal.get(Calendar.DAY_OF_MONTH);
		int h = cal.get(Calendar.HOUR_OF_DAY);
		int m = cal.get(Calendar.MONTH);
		int y = cal.get(Calendar.YEAR);
		/** Initialization of aggregators */
		String[] aggregators = conf.get("globalAggregators").split("\n");
		
		String dayHour = d + "-" + m + "-" + y + "-" + h;
			
		for(String aggregatorDescr : aggregators){
			Aggregator a = new Aggregator(aggregatorDescr);
			if(a.respects(dayHour)){
			/** newKey is PeriodId,PeriodDuration,SourceId,DestId */
			Text newKey = new Text(a.getIdentifier() +","+a.getAggregationLength(600)+","+ record[1].toString() + "," +record[2].toString());
			/** val is Strength */
			DoubleWritable val = new DoubleWritable(Double.parseDouble(record[3]));
			context.write(newKey,val);
			return;
			}
		}
			
	}
		
}