package task1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Task1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	final private static Logger Task1MapperLogger = Logger.getLogger(Task1Mapper.class);
	final private static IntWritable ONE = new IntWritable(1);
	private Text word = new Text();
	StringTokenizer tokenizer;

	@Override
	protected void map(LongWritable offset, Text valueIn, Context context) throws IOException, InterruptedException {
		// Set log-level to debug
		Task1MapperLogger.setLevel(Level.DEBUG);
		Task1MapperLogger.debug("The mapper task of Chenyu Xiao, s3829221");
		
		// Split document into tokens using default delimiters
		tokenizer = new StringTokenizer(valueIn.toString());
		
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			// Classify words based on length
			if (token.length() >= 1 && token.length() <= 4) {
				word.set("short");
			} else if (token.length() >= 5 && token.length() <= 7) {
				word.set("medium");
			} else if (token.length() >= 8 && token.length() <= 10) {
				word.set("long");
			} else {
				word.set("extra-long");
			}
			Task1MapperLogger.debug(token + " is classified into " + word.toString());
			
			// Emit intermediate key-value pair
			context.write(word, ONE);
		}
	}
}
