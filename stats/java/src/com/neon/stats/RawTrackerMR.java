package com.neon.stats;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class RawTrackerMR {
	
	public static class Map extends MapReduceBase implements 
	Mapper<LongWritable, Text, Text, NeonEventWritable> {
		
	}
	
	public static class Reduce extends MapReduceBase implements 
	Reducer<Text, NeonEventWritable, Text, IntWritable> {
		
	}
}