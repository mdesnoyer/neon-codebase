/**
 * 
 */
package com.neon.stats;

import java.io.IOException;
import java.util.*;

import org.apache.avro.generic.GenericData;
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
   *         Map all the events for a given user viewing a video on a given site
   *         together. We also expand the Image Load and Image Visible events to
   *         one for each image.
   * 
   */
  public static class MapToUserEventStream
      extends
      Mapper<AvroKey<TrackerEvent>, NullWritable, Text, AvroValue<TrackerEvent>> {

    @Override
    public void map(AvroKey<TrackerEvent> key, NullWritable value,
        Context context) throws IOException, InterruptedException {
      String mapKey =
          key.datum().getTrackerAccountId().toString()
              + key.datum().getClientIP();
      String videoId;

      switch (key.datum().getEventType()) {

      case IMAGES_VISIBLE:
        List<CharSequence> thumbnailids =
            ((ImagesVisible) key.datum().getEventData()).getThumbnailIds();
        for (CharSequence thumbnailId : thumbnailids) {
          // We don't copy the original event because it doesn't need to be. The
          // new events are copied when they are written.
          ImageVisible newEventData = new ImageVisible(thumbnailId);
          TrackerEvent newEvent = key.datum();
          newEvent.setEventData(newEventData);
          newEvent.setEventType(EventType.IMAGE_VISIBLE);
          context.write(new Text(mapKey
              + ExtractVideoId(thumbnailId.toString())),
              new AvroValue<TrackerEvent>(newEvent));
        }
        break;

      case IMAGES_LOADED:
        List<ImageLoad> imageLoads =
            ((ImagesLoaded) key.datum().getEventData()).getImages();
        for (ImageLoad imageLoad : imageLoads) {
          // We don't copy the original event because it doesn't need to be. The
          // new events are copied when they are written.
          TrackerEvent newEvent = key.datum();
          newEvent.setEventData(imageLoad);
          newEvent.setEventType(EventType.IMAGE_LOAD);
          context.write(new Text(mapKey
              + ExtractVideoId(imageLoad.getThumbnailId().toString())),
              new AvroValue<TrackerEvent>(newEvent));
        }
        break;

      case IMAGE_CLICK:
        String thumbnailId =
            ((ImageClick) key.datum().getEventData()).getThumbnailId()
                .toString();
        context.write(new Text(mapKey + ExtractVideoId(thumbnailId)),
            new AvroValue<TrackerEvent>(key.datum()));
        break;

      case VIDEO_CLICK:
        videoId =
            ((VideoClick) key.datum().getEventData()).getVideoId().toString();
        context.write(new Text(mapKey + videoId), new AvroValue<TrackerEvent>(
            key.datum()));
        break;

      case VIDEO_PLAY:
        videoId =
            ((VideoPlay) key.datum().getEventData()).getVideoId().toString();
        context.write(new Text(mapKey + videoId), new AvroValue<TrackerEvent>(
            key.datum()));
        break;

      case AD_PLAY:
        // The video id could be null in an ad play. For now don't try to figure
        // out the associated video id
        // TODO(mdesnoyer): Figure out the video idea in this case.
        if (((AdPlay) key.datum().getEventData()).getVideoId() == null) {
          context.write(new Text(mapKey),
              new AvroValue<TrackerEvent>(key.datum()));
        } else {
          videoId =
              ((AdPlay) key.datum().getEventData()).getVideoId().toString();
          context.write(new Text(mapKey + videoId),
              new AvroValue<TrackerEvent>(key.datum()));
        }
        break;
        
      default:
        context.getCounter("MappingError", "InvalidEvent").increment(1);
      }
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
      // Grab all the events and sort them by client time so that we can
      // analyze
      // the stream.
      Vector<TrackerEvent> events = new Vector<TrackerEvent>();
      for (AvroValue<TrackerEvent> value : values) {
        events.add(value.datum());
      }
      Collections.sort(events, new Comparator<TrackerEvent>() {
        public int compare(TrackerEvent a, TrackerEvent b) {
          return (int) (a.getClientTime() - b.getClientTime());
        }
      });

    }

    protected void cleanup(Context context) throws IOException,
        InterruptedException {
      out.close();
    }
  }

  private static String ExtractVideoId(String thumbnailId) {
    if (thumbnailId == null) {
      return null;
    }
    return thumbnailId.split("_")[1];
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
    int res = ToolRunner.run(new RawTrackerMR(), args);
    System.exit(res);
  }

}
