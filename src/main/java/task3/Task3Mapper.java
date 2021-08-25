package task3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Task3Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	final private static Logger Task1MapperLogger = Logger.getLogger(Task3Mapper.class);
	final private static IntWritable ONE = new IntWritable(1);
	private Text word = new Text();
	StringTokenizer tokenizer;
	private Map<Text, IntWritable> associativeArray = new HashMap<>();

	@Override
	protected void map(LongWritable offset, Text valueIn, Context context) {
		// Set log-level to debug
		Task1MapperLogger.setLevel(Level.DEBUG);
		Task1MapperLogger.debug("The mapper task of Chenyu Xiao, s3829221");

		// Split document into tokens using default delimiters
		tokenizer = new StringTokenizer(valueIn.toString());

		// Update associative array
		while (tokenizer.hasMoreTokens()) {
			Text token = new Text(tokenizer.nextToken());
			if (associativeArray.containsKey(token)) {
				int count = associativeArray.get(token).get() + 1;
				associativeArray.put(token, new IntWritable(count));
			} else {
				associativeArray.put(token, ONE);
			}
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		// Emit intermediate key-value pairs
		for (Map.Entry<Text, IntWritable> entry : associativeArray.entrySet()) {
			context.write(entry.getKey(), entry.getValue());
		}
	}
}
