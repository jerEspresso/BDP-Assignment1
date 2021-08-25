package task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Task1Partitioner extends Partitioner<Text, IntWritable> {
	
	final private static Logger Task1PartitionerLogger = Logger.getLogger(Task1Partitioner.class);

	@Override
	public int getPartition(Text keyIn, IntWritable valueIn, int numPartitions) {
		// Set log-level to debug
		Task1PartitionerLogger.setLevel(Level.DEBUG);
		
		if (numPartitions == 0) {
			Task1PartitionerLogger.debug("Only one reducer (no partitioning required)");
			return 0;
		}
		
		// Assign short and medium words to the same reducer
		if (keyIn.toString().equals("short") || keyIn.toString().equals("medium")) {
			Task1PartitionerLogger.debug(keyIn.toString() + " is assigned to reducer 0");
			return 0;
		}
		// Assign long and extra-long words to the same reducer
		else {
			Task1PartitionerLogger.debug(keyIn.toString() + " is assigned to reducer " + 1 % numPartitions);
			return 1 % numPartitions;
		}
			
	}

}
