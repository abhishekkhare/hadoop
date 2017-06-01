package com.edu.abhi.hadoop.itversity;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.edu.abhi.hadoop.util.FileUtils;

/**
 * 
 * @author abhishekkhare
 *
 */
public class CardCountBySuit extends Configured implements Tool {

private static class CardCountBySuitMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		@Override
		public  void map(LongWritable lineOffSet, Text record, Context context) throws IOException, InterruptedException{
			context.write(new Text(record.toString().split("\\|")[1]), new LongWritable(1));
		}
	}
	
	
	public int run(String[] args) throws Exception {
		FileUtils.deleteDirectory(new File("/Users/abhishekkhare/Google Drive/Ebooks/learnworkspace/hadoop/hadoop/output"));
		Job job = Job.getInstance(getConf());
		//Mapper Configuration
		job.setMapperClass(CardCountBySuitMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setPartitionerClass(HashPartitioner.class);
		
		//Reducer Configuration
		job.setReducerClass(LongSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setNumReduceTasks(2);

		
		FileInputFormat.setInputPaths(job, new Path("/Users/abhishekkhare/Google Drive/Ebooks/learnworkspace/hadoop/hadoop/data/deckofcards.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/Users/abhishekkhare/Google Drive/Ebooks/learnworkspace/hadoop/hadoop/output/cardCountBySuit"));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		System.exit(ToolRunner.run(new CardCountBySuit(), args));
	}
}
