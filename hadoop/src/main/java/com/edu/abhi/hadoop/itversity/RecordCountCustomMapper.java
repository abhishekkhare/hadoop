package com.edu.abhi.hadoop.itversity;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.edu.abhi.hadoop.util.FileUtils;

/**
 * 
 * @author abhishekkhare
 *
 */
public class RecordCountCustomMapper extends Configured implements Tool {
	
	private static class RecordMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		
		@Override
		public  void map(LongWritable lineOffSet, Text record, Context output) throws IOException, InterruptedException{
			output.write(new Text("Count"), new LongWritable(1));
		}
	}
	
	public int run(String[] args) throws Exception {
		FileUtils.deleteDirectory(new File("/Users/abhishekkhare/Google Drive/Ebooks/learnworkspace/hadoop/hadoop/output"));
		if(args!=null && args[1]!=null)
			FileUtils.deleteDirectoryOnHDFS(args[1]);
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(getClass());
		
		job.setMapperClass(RecordMapper.class);
		job.setNumReduceTasks(0);	
		
		if(args!=null && args[0]!=null){
			FileInputFormat.setInputPaths(job, new Path(args[0]));	
		}else{
			FileInputFormat.setInputPaths(job, new Path("/Users/abhishekkhare/Google Drive/Ebooks/learnworkspace/hadoop/hadoop/data/deckofcards.txt"));
		}
		if(args!=null && args[1]!=null){
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
		}else{
			FileUtils.deleteDirectory(new File("/Users/abhishekkhare/Google Drive/Ebooks/learnworkspace/hadoop/hadoop/output"));
			FileOutputFormat.setOutputPath(job, new Path("/Users/abhishekkhare/Google Drive/Ebooks/learnworkspace/hadoop/hadoop/output/recordCountWithMapperOnly"));	
		}
		
		
		
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String [] args) throws Exception{
		
		System.exit(ToolRunner.run(new RecordCountCustomMapper(), args));
	}
}
