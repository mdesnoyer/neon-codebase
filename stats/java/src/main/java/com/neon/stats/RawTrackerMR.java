/**
 * 
 */
package com.neon.stats;

import java.io.IOException;
import java.util.*;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.neon.Tracker.*;

/**
 * @author mdesnoyer
 * 
 *         MapReduce job that cleans the raw tracker data and puts it in
 *         different streams that can be imported directly as a Hive table.
 * 
 */
public class RawTrackerMR extends Configured implements Tool {

  /**
   * @author mdesnoyer
   * 
   *         Map all the events for a given user on a given site together.
   * 
   */
  public static class MapToUserEventStream
      extends
      Mapper<AvroKey<TrackerEvent>, NullWritable, Text, AvroValue<TrackerEvent>> {

    @Override
    public void map(AvroKey<TrackerEvent> key, NullWritable value,
        Context context) throws IOException, InterruptedException {

      String mapKey = key.datum().getTrackerAccountId().toString()
          + key.datum().getClientIP();

      context.write(new Text(mapKey), new AvroValue<TrackerEvent>(key.datum()));
    }
  }

  /**
   * @author mdesnoyer
   * 
   *         Cleans the tracker events by looking at the entire stream of data
   *         from a given user and makes it consistent by either cleaning data
   *         and/or backfilling information.
   * 
   */
  public static class CleanUserStreamReducer
      extends
      Reducer<Text, AvroValue<TrackerEvent>, WritableComparable<Object>, Writable> {

    private MultipleOutputs<WritableComparable<Object>, Writable> out;

    public void setup(Context context) {
      out = new MultipleOutputs<WritableComparable<Object>, Writable>(context);
    }

    @Override
    public void reduce(Text key, Iterable<AvroValue<TrackerEvent>> values,
        Context context) throws IOException, InterruptedException {
      // Grab all the events and sort them by client time so that we can analyze
      // the stream.
      Vector<TrackerEvent> events = new Vector<TrackerEvent>();
      for (AvroValue<TrackerEvent> value : values) {
        events.add(value.datum());
      }
      Collections.sort(events, new Comparator<TrackerEvent>() {
        public int compare(TrackerEvent a, TrackerEvent b) {
          return (int)(a.getClientTime() - b.getClientTime());
        }
      });
      
    }

    protected void cleanup(Context context) throws IOException,
        InterruptedException {
      out.close();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: RawTrackerMR <input path> <output path>");
      return -1;
    }

    Job job = new Job(getConf());
    job.setJarByClass(RawTrackerMR.class);
    job.setJobName("Raw Tracker Data Cleaning");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // Define the mapper
    job.setMapperClass(MapToUserEventStream.class);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    AvroJob.setInputKeySchema(job, TrackerEvent.getClassSchema());
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(AvroValue.class);

    // Define the reducer
    job.setReducerClass(CleanUserStreamReducer.class);

    // Define the output streams, one per table
    AvroMultipleOutputs.addNamedOutput(job, "imageLoad",
        AvroKeyOutputFormat.class, ImageLoadEvent.getClassSchema());
    AvroMultipleOutputs.addNamedOutput(job, "imageVisible",
        AvroKeyOutputFormat.class, ImageVisibleEvent.getClassSchema());
    AvroMultipleOutputs.addNamedOutput(job, "imageClick",
        AvroKeyOutputFormat.class, ImageClickEvent.getClassSchema());
    AvroMultipleOutputs.addNamedOutput(job, "adPlay",
        AvroKeyOutputFormat.class, AdPlayEvent.getClassSchema());
    AvroMultipleOutputs.addNamedOutput(job, "videoPlay",
        AvroKeyOutputFormat.class, VideoPlayEvent.getClassSchema());

    return (job.waitForCompletion(true) ? 0 : 1);
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    int res = ToolRunner.run(new RawTrackerMR(), args);
    System.exit(res);
  }

}
