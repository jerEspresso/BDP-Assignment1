package task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Task2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	final private static Logger Task1ReducerLogger = Logger.getLogger(Task2Reducer.class);
	
	
	@Override
	protected void reduce(Text keyIn, Iterable<IntWritable> valuesIn, Context context) throws IOException, InterruptedException {
		int valueOut = 0;
		
		// Set log-level to debug
		Task1ReducerLogger.setLevel(Level.DEBUG);
		Task1ReducerLogger.debug("The reducer task of Chenyu Xiao, s3829221");
		
		// Aggregate values of the same key
		for (IntWritable value : valuesIn) {
			valueOut += value.get();
		}
		
		context.write(keyIn, new IntWritable(valueOut));
	}
}
