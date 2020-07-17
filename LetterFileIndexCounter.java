package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class LetterFileIndexCounter
{
	public static class Map_A extends MapReduceBase implements Mapper<LongWritable, Text, LetterFilenameCompositeKey, IntWritable> 
	{
		private final static IntWritable one = new IntWritable(1);
		private LetterFilenameCompositeKey lfck = new LetterFilenameCompositeKey();

		public void map(LongWritable key, Text value, OutputCollector<LetterFilenameCompositeKey, IntWritable> output, Reporter reporter) throws IOException 
		{
			String filename = ((FileSplit) reporter.getInputSplit()).getPath().getName();	// fetch the name of the current file
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			// for every token/word...
			while (tokenizer.hasMoreTokens()) 
			{
				// set the first character (converted to uppercase) of the word and the filename ((letter, filename), 1) as the key 
				// and '1' as the default value
				lfck.set(String.valueOf(tokenizer.nextToken().charAt(0)).toUpperCase(), filename);
				output.collect(lfck, one);
			}
		}
	}



	public static class Reduce_A extends MapReduceBase implements Reducer<LetterFilenameCompositeKey, IntWritable, LetterFilenameCompositeKey, IntWritable> 
	{
		public void reduce(LetterFilenameCompositeKey key, Iterator<IntWritable> values, OutputCollector<LetterFilenameCompositeKey, IntWritable> output, Reporter reporter) throws IOException 
		{
			int sum = 0;

			// for every key-value pair that has the same key...
			while (values.hasNext()) 
				sum += values.next().get();	// reduce by adding all the values together

			// store the resulting key-value pair ((letter, filename), sum) inside the output directory
			output.collect(key, new IntWritable(sum));	
		}
	}



	public static class Map_B extends MapReduceBase implements Mapper<LongWritable, Text, Text, FilenameSumCompositeKey> 
	{
		private Text letter = new Text();
		private FilenameSumCompositeKey fsck = new FilenameSumCompositeKey();

		public void map(LongWritable key, Text value, OutputCollector<Text, FilenameSumCompositeKey> output, Reporter reporter) throws IOException 
		{
			String[] lines = value.toString().split("\\r?\\n");	// strip the newlines from the files in the /inbetween/ directory

			// for every line in the file...
			for(String line : lines)
			{
				String[] columns = line.split("\\s+"); // fetch the columns with the results from the first round

				// create a composite key-value pair where key is the letter from the second column
				// and the value is the filename from the first column combined with the calculated sum from before (letter, (filename, sum))
				letter.set(columns[1]);
				fsck.set(columns[0], columns[2]);

				output.collect(letter, fsck);
			}
		}
	}


	public static class Reduce_B extends MapReduceBase implements Reducer<Text, FilenameSumCompositeKey, Text, FilenameSumCompositeKey> 
	{
		public void reduce(Text key, Iterator<FilenameSumCompositeKey> values, OutputCollector<Text, FilenameSumCompositeKey> output, Reporter reporter) throws IOException 
		{
			int max = 0;
			String filename = "";
			FilenameSumCompositeKey current_value;

			// for every key-value pair that has the same key...
			while (values.hasNext()) 
			{
				current_value = values.next();

				// reduce by finding the max of all the sums for the pairs with the same key
				if(Integer.valueOf(current_value.getSum()) > max)
				{
					max = Integer.valueOf(current_value.getSum());
					filename = current_value.getFilename();
				}
			}

			// store the resulting key-value pair (letter, (filename, max(sum)) inside the output directory
			output.collect(key, new FilenameSumCompositeKey(filename, String.valueOf(max)));	
		}
	}


	public static void main(String[] args) throws Exception 
	{
		/* driver for the 1st round of MR job */
		JobConf conf_1 = new JobConf(LetterFileIndexCounter.class);
		conf_1.setJobName("LetterFileIndexCounter_step_A");

		conf_1.setOutputKeyClass(LetterFilenameCompositeKey.class);
		conf_1.setOutputValueClass(IntWritable.class);

		conf_1.setMapperClass(Map_A.class);
		conf_1.setCombinerClass(Reduce_A.class);
		conf_1.setReducerClass(Reduce_A.class);

		conf_1.setInputFormat(TextInputFormat.class);
		conf_1.setOutputFormat(TextOutputFormat.class);

		conf_1.setNumReduceTasks(1); 	// number of reducers to run on the first round of the application

		// the path of the input and inbetween directories in hadoop's file system
		FileInputFormat.setInputPaths(conf_1, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf_1, new Path(args[1]));

		JobClient.runJob(conf_1);

		/* driver for the 2nd round MR job */
		JobConf conf_2 = new JobConf(LetterFileIndexCounter.class);
		conf_2.setJobName("LetterFileIndexCounter_step_B");

		conf_2.setOutputKeyClass(Text.class);
		conf_2.setOutputValueClass(FilenameSumCompositeKey.class);

		conf_2.setMapperClass(Map_B.class);
		conf_2.setCombinerClass(Reduce_B.class);
		conf_2.setReducerClass(Reduce_B.class);

		conf_2.setInputFormat(TextInputFormat.class);
		conf_2.setOutputFormat(TextOutputFormat.class);

		conf_2.setNumReduceTasks(1);	// number of reducers to run on the second round of the application

		// the path of the inbetween and output directories in hadoop's file system
		FileInputFormat.setInputPaths(conf_2, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf_2, new Path(args[2]));
		JobClient.runJob(conf_2);
	}
}
