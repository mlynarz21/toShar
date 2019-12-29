import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BikeReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		Text val = new Text();

		// Go through all values to sum up card values for a card suit
		for (Text value : values) {
			val = value;
		}

		context.write(key, val);
	}
}
