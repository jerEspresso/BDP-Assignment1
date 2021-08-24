package s3829221.s3829221_S2_2020;

import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task1 {

	public static class Task1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		final private static LongWritable ONE = new LongWritable(1);
		private Text word = new Text();
		StringTokenizer tokenizer;

		@Override
		protected void map(LongWritable offset, Text valueIn, Context context) {
			tokenizer = new StringTokenizer(valueIn.toString());
			
			while (tokenizer.hasMoreTokens()) {
				// Classify words based on length
				if (tokenizer.nextToken().length() >= 1 && tokenizer.nextToken().length() <= 4) {
					word.set("short");
				} else if (tokenizer.nextToken().length() >= 5 && tokenizer.nextToken().length() <= 7) {
					word.set("medium");
				} else if (tokenizer.nextToken().length() >= 8 && tokenizer.nextToken().length() <= 10) {
					word.set("long");
				} else {
					word.set("extra-long");
				}
				
			}
		}
	}

	public static void main(String[] args) {
		System.out.println("Hello World!");
	}
}
