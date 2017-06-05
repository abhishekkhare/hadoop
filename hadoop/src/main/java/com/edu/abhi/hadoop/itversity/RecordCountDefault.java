package com.edu.abhi.hadoop.itversity;

import java.io.File;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
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
public class RecordCountDefault extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		FileUtils.deleteDirectory(new File("/Users/abhishekkhare/Google Drive/Ebooks/learnworkspace/hadoop/hadoop/output/RC"));
		if(args!=null && args[1]!=null)
			FileUtils.deleteDirectoryOnHDFS(args[1]);
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(getClass());
		
		/**
		 * Below are the default configurations.
		 * The default mapper is Mapper.class, the default reducer is Reducer.class, 
		 * the default input format is text and the default output format is text.
		 * Here we have done this explicitly to show how all the confgurations are done.
		 */
		//job.setMapperClass(Mapper.class);
		//job.setReducerClass(Reducer.class);
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		//FileInputFormat.setInputPaths(job, new Path("/Users/abhishekkhare/Downloads/largedeck.txt"));
		if(args!=null && args[0]!=null){
			FileInputFormat.setInputPaths(job, new Path(args[0]));	
		}else{
			FileInputFormat.setInputPaths(job, new Path("/Users/abhishekkhare/Google Drive/Ebooks/learnworkspace/hadoop/hadoop/data/deckofcards.txt"));
		}
		if(args!=null && args[1]!=null){
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
		}else{
			FileUtils.deleteDirectory(new File("/Users/abhishekkhare/Google Drive/Ebooks/learnworkspace/hadoop/hadoop/output"));
			FileOutputFormat.setOutputPath(job, new Path("/Users/abhishekkhare/Google Drive/Ebooks/learnworkspace/hadoop/hadoop/output/RC"));	
		}
		
		
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String [] args) throws Exception{
		
		System.exit(ToolRunner.run(new RecordCountDefault(), args));
	}
}
