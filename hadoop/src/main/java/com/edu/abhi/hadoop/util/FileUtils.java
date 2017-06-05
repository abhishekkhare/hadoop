package com.edu.abhi.hadoop.util;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
/**
 * 
 * @author abhishekkhare
 *
 */
public class FileUtils {
	
	public static boolean deleteDirectory(File dir) {
		if (dir.isDirectory()) {
			File[] children = dir.listFiles();
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDirectory(children[i]);
				if (!success) {
					return false;
				}
			}
		}
		// either file or an empty directory
		System.out.println("removing file or directory : " + dir.getName());
		return dir.delete();
	}
	
	public static boolean deleteDirectoryOnHDFS(String path) throws IOException{
		Configuration conf = new Configuration();

		Path output = new Path("/the/folder/to/delete");
		FileSystem hdfs = FileSystem.get(conf);

		// delete existing directory
		if (hdfs.exists(output)) {
		    return hdfs.delete(output, true);
		}
		
		return false;
	}
}
