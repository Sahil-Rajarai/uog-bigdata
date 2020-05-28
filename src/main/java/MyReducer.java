

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/*
 * The Reducer is modified such that it takes the value as a IntWritable value. 
*/
public class MyReducer extends Reducer<Text, IntWritable, Text, Text> {
	private CompositeValue _value;
	MultipleOutputs multipleOutputs;
	
	@Override
	protected void setup(Context context){
		// TODO Auto-generated method stub
		multipleOutputs = new MultipleOutputs(context);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		// TODO Auto-generated method stub
		multipleOutputs.close();
	}

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context
			context) throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable val : values) {
			sum = sum + 1;
		}
		//scenario 1, calculating ft,d
		if(key.toString().startsWith("t_")) {
			String[] lstDocID = key.toString().split("d_");
			CompositeValue val = new CompositeValue(lstDocID[1],sum);
			String key_new = lstDocID[0].replace("t_", "");
			multipleOutputs.write("wordFreq",key_new, new Text(val.toString()));
		}
		//scenario 2, calculating |d|
		else {
			String key_new = key.toString().replace("d_", "");
			multipleOutputs.write("docLength", key_new, new Text(String.valueOf(sum)));
		}
		
	}
}