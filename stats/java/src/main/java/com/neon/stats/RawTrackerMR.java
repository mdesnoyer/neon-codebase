/**
 * 
 */
package com.neon.stats;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.neon.Tracker.*;

/**
 * @author mdesnoyer
 * 
 *         MapReduce job that cleans the raw tracker data and puts it in
 *         different streams that can be imported directly as a Hive table.
 * 
 */
public class RawTrackerMR extends Configured implements Tool {
  // The maximum time that a sequence can occur over (6 hours)
  private static final long MAX_SEQUENCE_TIME = 21600000;

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
        // TODO(mdesnoyer): Figure out the video id in this case.
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
      Reducer<Text, AvroValue<TrackerEvent>, WritableComparable<Object>, NullWritable> {

    private AvroMultipleOutputs out;

    /**
     * Constructor primarily for unittesting
     * 
     * @param _out
     */
    public CleanUserStreamReducer(AvroMultipleOutputs _out) {
      this.out = _out;
    }

    public CleanUserStreamReducer() {
      this.out = null;
    }

    public void setup(Context context) {
      if (this.out == null) {
        out = new AvroMultipleOutputs(context);
      }
    }

    @Override
    public void reduce(Text key, Iterable<AvroValue<TrackerEvent>> values,
        Context context) throws IOException, InterruptedException {
      // Grab all the events and sort them by client time so that we can
      // analyze the stream.
      ArrayList<TrackerEvent> events = new ArrayList<TrackerEvent>();
      for (AvroValue<TrackerEvent> value : values) {
        events.add(TrackerEvent.newBuilder(value.datum()).build());
      }
      Collections.sort(events, new Comparator<TrackerEvent>() {
        public int compare(TrackerEvent a, TrackerEvent b) {
          int timeDiff = (int) (a.getClientTime() - b.getClientTime());
          if (timeDiff == 0) {
            // Force the event types to be clustered
            return a.getEventType().compareTo(b.getEventType());
          }
          return timeDiff;
        }
      });

      // Build up the list of events as they will be in their Hive tables
      LinkedList<Pair<TrackerEvent, Long>> seqEvents =
          new LinkedList<Pair<TrackerEvent, Long>>();
      for (ListIterator<TrackerEvent> baseI = events.listIterator(); baseI
          .hasNext();) {
        TrackerEvent curEvent = baseI.next();
        long curSequenceId = InitializeSequenceId(curEvent, context);

        // Skip this event if it is a duplicate
        ListIterator<Pair<TrackerEvent, Long>> revIter =
            seqEvents.listIterator(seqEvents.size());
        TrackerEvent oldEvent;
        boolean foundDup = false;
        while (revIter.hasPrevious()) {
          oldEvent = revIter.previous().getLeft();
          if (IsDuplicateTrackerEvent(oldEvent, curEvent)) {
            foundDup = true;
            break;
          } else if ((curEvent.getClientTime() - oldEvent.getClientTime()) > MAX_SEQUENCE_TIME) {
            // Stop looking for duplicates that are too old
            break;
          }
        }
        if (foundDup) {
          context.getCounter("EventStats", "DuplicatesFound").increment(1);
          break;
        }

        BackfillSequenceId(curEvent, seqEvents, curSequenceId, context);

        // If it is a video click, see if there is an associated image click. If
        // so, ignore the video click.
        if (curEvent.getEventType() == EventType.VIDEO_CLICK) {
          // See if we can find an associated image click on the same page
          revIter = seqEvents.listIterator(seqEvents.size());
          boolean foundRealClick = false;
          while (revIter.hasPrevious() && !foundRealClick) {
            oldEvent = revIter.previous().getLeft();
            if (oldEvent.getPageId().equals(curEvent.getPageId())) {
              if (oldEvent.getEventType() == EventType.IMAGE_CLICK) {
                foundRealClick = true;
                break;
              }
            } else if (oldEvent.getEventType() == EventType.VIDEO_PLAY
                || oldEvent.getEventType() == EventType.VIDEO_CLICK) {
              break;
            } else if ((curEvent.getClientTime() - oldEvent.getClientTime()) > MAX_SEQUENCE_TIME) {
              // Stop looking because it's too old
              break;
            }
          }
          if (foundRealClick) {
            // There is a real click so we can ignore this video click
            context.getCounter("EventStats", "ExtraVideoClicks").increment(1);
            break;
          }

        }

        // Add the current event to the ones to write out
        seqEvents.add(MutablePair.of(curEvent, curSequenceId));

      }

      // Now output the resulting events.
      for (Pair<TrackerEvent, Long> pair : seqEvents) {
        OutputEventToHive(pair.getLeft(), pair.getRight(), context);
      }
    }

    protected void cleanup(Context context) throws IOException,
        InterruptedException {
      out.close();
    }

    private static long
        InitializeSequenceId(TrackerEvent event, Context context) {
      long videoPlayHash = 0;
      CharSequence videoId;

      switch (event.getEventType()) {
      case IMAGE_VISIBLE:
        videoPlayHash =
            ((ImageVisible) event.getEventData()).getThumbnailId().hashCode();
        break;
      case IMAGE_LOAD:
        videoPlayHash =
            ((ImageLoad) event.getEventData()).getThumbnailId().hashCode();
        break;
      case IMAGE_CLICK:
        videoPlayHash =
            ((ImageClick) event.getEventData()).getThumbnailId().hashCode();
        break;
      case VIDEO_CLICK:
        videoPlayHash =
            ((VideoClick) event.getEventData()).getVideoId().hashCode();
        break;
      case AD_PLAY:
        videoPlayHash = ((AdPlay) event.getEventData()).getPlayCount();
        videoId = ((AdPlay) event.getEventData()).getVideoId();
        if (videoId != null) {
          videoPlayHash ^= videoId.hashCode();
        }
        break;
      case VIDEO_PLAY:
        videoPlayHash =
            ((VideoPlay) event.getEventData()).getPlayCount()
                ^ ((VideoPlay) event.getEventData()).getVideoId().hashCode();
        break;
      default:
        context.getCounter("ReduceError", "InvalidEventType").increment(1);
      }

      return ((long) event.getPageId().hashCode()) << 32 | videoPlayHash;
    }

    private static void BackfillSequenceId(TrackerEvent curEvent,
        LinkedList<Pair<TrackerEvent, Long>> seqEvents, long curSequenceId,
        Context context) {
      Pair<TrackerEvent, Long> eventPair;
      if (curEvent.getEventType() == EventType.AD_PLAY
          || curEvent.getEventType() == EventType.VIDEO_PLAY) {
        boolean fromOtherPage =
            curEvent.getRefURL() != null && IsFirstAutoplay(curEvent);
        boolean isAutoplay = IsAutoplay(curEvent);

        boolean foundClick = false;
        CharSequence foundThumbnailId = null;
        CharSequence clickPageId = null;
        boolean doneBackfill = false;
        ListIterator<Pair<TrackerEvent, Long>> revIter =
            seqEvents.listIterator(seqEvents.size());
        while (revIter.hasPrevious() && !doneBackfill) {
          eventPair = revIter.previous();
          TrackerEvent oldEvent = eventPair.getLeft();

          if (oldEvent.getPageId().equals(curEvent.getPageId())) {
            if (isAutoplay) {
              // If this is a video click, we can remove it because we never
              // care about video clicks when an autoplay happens
              if (oldEvent.getEventType() == EventType.VIDEO_CLICK) {
                revIter.remove();
                context.getCounter("EventStats", "ExtraVideoClicks").increment(
                    1);
              }
            } else {
              // Events occured on the same page load so if it's not an
              // autoclick, then transfer the sequence id
              switch (oldEvent.getEventType()) {
              case VIDEO_CLICK:
                if (foundClick) {
                  // Remove the video click because there was an image click
                  revIter.remove();
                  context.getCounter("EventStats", "ExtraVideoClicks")
                      .increment(1);
                  break;
                }
                foundClick = true;
                foundThumbnailId =
                    ((VideoClick) oldEvent.getEventData()).getThumbnailId();
                eventPair.setValue(curSequenceId);
                clickPageId = oldEvent.getPageId();
                break;
              case IMAGE_CLICK:
                foundClick = true;
                foundThumbnailId =
                    ((ImageClick) oldEvent.getEventData()).getThumbnailId();
                eventPair.setValue(curSequenceId);
                clickPageId = oldEvent.getPageId();
                break;
              case IMAGE_LOAD:
              case IMAGE_VISIBLE:
                eventPair.setValue(curSequenceId);
                break;
              case VIDEO_PLAY:
                // We've seen another video play so we can stop looking
                doneBackfill = true;
              default:
                // Nothing to do

              }
            }
          } else if (fromOtherPage
              && oldEvent.getPageURL().equals(curEvent.getRefURL())) {
            // We have an event from the page that referred us to backfill the
            // sequence id.
            switch (oldEvent.getEventType()) {
            case IMAGE_CLICK:
              if (foundClick) {
                doneBackfill = true;
                break;
              }
              foundClick = true;
              foundThumbnailId =
                  ((ImageClick) oldEvent.getEventData()).getThumbnailId();
              clickPageId = oldEvent.getPageId();
            case IMAGE_LOAD:
            case IMAGE_VISIBLE:
              // Only backfill for the most recent page load of the referral
              if (clickPageId != null
                  && clickPageId.equals(oldEvent.getPageId())) {
                eventPair.setValue(curSequenceId);
              }
            default:
              // We don't care about other events
            }
          }

          // Stop the backfill if the data is too old
          doneBackfill =
              (curEvent.getClientTime() - oldEvent.getClientTime()) > MAX_SEQUENCE_TIME;
        }

        // Fill in the thumbnail id from the one we found in the previous event
        if (foundThumbnailId != null) {
          if (curEvent.getEventType() == EventType.AD_PLAY) {
            ((AdPlay) curEvent.getEventData()).setThumbnailId(foundThumbnailId);
          } else if (curEvent.getEventType() == EventType.VIDEO_PLAY) {
            ((VideoPlay) curEvent.getEventData())
                .setThumbnailId(foundThumbnailId);
          }
        }
      }
    }

    /**
     * Returns true if two events are functionally duplicates.
     */
    private static boolean IsDuplicateTrackerEvent(TrackerEvent a,
        TrackerEvent b) {
      if (a.getEventType() != b.getEventType()) {
        return false;
      }

      switch (a.getEventType()) {
      case IMAGE_LOAD:
      case IMAGE_VISIBLE:
        return a.getPageId().equals(b.getPageId());

      case IMAGE_CLICK:
      case VIDEO_CLICK:
      case VIDEO_PLAY:
      case AD_PLAY:
        // TODO(mdesnoyer): Maybe flag play & click events as duplicates if they
        // are too close in time
        return a.equals(b);

      }

      return true;
    }

    /**
     * Returns true if the play event was the result of an autoplay
     */
    private static boolean IsAutoplay(TrackerEvent event) {
      if (event.getEventType() == EventType.VIDEO_PLAY) {
        VideoPlay data = (VideoPlay) event.getEventData();
        return data.getAutoplayDelta() == null
            || data.getAutoplayDelta() > 2000;
      } else if (event.getEventType() == EventType.AD_PLAY) {
        AdPlay adData = (AdPlay) event.getEventData();
        return adData.getAutoplayDelta() == null
            || adData.getAutoplayDelta() > 2000;
      }

      return false;
    }

    /**
     * Returns true if the play event was the result of an autoplay and it's the
     * first on a page
     */
    private static boolean IsFirstAutoplay(TrackerEvent event) {
      int playCount = -1;
      if (event.getEventType() == EventType.VIDEO_PLAY) {
        playCount = ((VideoPlay) event.getEventData()).getPlayCount();
      } else if (event.getEventType() == EventType.AD_PLAY) {
        playCount = ((AdPlay) event.getEventData()).getPlayCount();
      }

      return playCount == 1 && IsAutoplay(event);
    }

    private void OutputEventToHive(TrackerEvent orig, long sequenceId,
        Context context) throws IOException, InterruptedException {
      SpecificRecordBase hiveEvent;
      Coords clickCoords;

      // Create the output path that partitions based on time and the account id
      SimpleDateFormat timestampFormat = new SimpleDateFormat("YYYY-MM-dd");
      String timestamp = timestampFormat.format(new Date(orig.getServerTime()));
      String partitionPath =
          "/tai=" + orig.getTrackerAccountId() + "/ts=" + timestamp;

      switch (orig.getEventType()) {
      case IMAGE_LOAD:
        hiveEvent =
            ImageLoadHive
                .newBuilder()
                .setAgentInfo(orig.getAgentInfo())
                .setClientIP(orig.getClientIP())
                .setClientTime((float) (orig.getClientTime() / 1000.))
                .setIpGeoData(orig.getIpGeoData())
                .setNeonUserId(orig.getNeonUserId())
                .setPageId(orig.getPageId())
                .setPageURL(orig.getPageURL())
                .setRefURL(orig.getRefURL())
                .setServerTime((float) (orig.getServerTime() / 1000.))
                .setTrackerAccountId(orig.getTrackerAccountId())
                .setTrackerType(orig.getTrackerType())
                .setUserAgent(orig.getUserAgent())
                .setSequenceId(sequenceId)
                .setThumbnailId(
                    ((ImageLoad) orig.getEventData()).getThumbnailId())
                .setHeight(((ImageLoad) orig.getEventData()).getHeight())
                .setWidth(((ImageLoad) orig.getEventData()).getWidth())
                .build();
        out.write("ImageLoadHive", hiveEvent, NullWritable.get(),
            "ImageLoadHive" + partitionPath);
        break;

      case IMAGE_VISIBLE:
        hiveEvent =
            ImageVisibleHive
                .newBuilder()
                .setAgentInfo(orig.getAgentInfo())
                .setClientIP(orig.getClientIP())
                .setClientTime((float) (orig.getClientTime() / 1000.))
                .setIpGeoData(orig.getIpGeoData())
                .setNeonUserId(orig.getNeonUserId())
                .setPageId(orig.getPageId())
                .setPageURL(orig.getPageURL())
                .setRefURL(orig.getRefURL())
                .setServerTime((float) (orig.getServerTime() / 1000.))
                .setTrackerAccountId(orig.getTrackerAccountId())
                .setTrackerType(orig.getTrackerType())
                .setUserAgent(orig.getUserAgent())
                .setSequenceId(sequenceId)
                .setThumbnailId(
                    ((ImageVisible) orig.getEventData()).getThumbnailId())
                .build();
        out.write("ImageVisibleHive", hiveEvent, NullWritable.get(),
            "ImageVisibleHive" + partitionPath);
        break;

      case IMAGE_CLICK:
        // We know if it is a right click only if the click coordinates are 0.
        clickCoords = ((ImageClick) orig.getEventData()).getPageCoords();
        CharSequence thumbnailId =
            ((ImageClick) orig.getEventData()).getThumbnailId();
        hiveEvent =
            ImageClickHive
                .newBuilder()
                .setAgentInfo(orig.getAgentInfo())
                .setClientIP(orig.getClientIP())
                .setClientTime((float) (orig.getClientTime() / 1000.))
                .setIpGeoData(orig.getIpGeoData())
                .setNeonUserId(orig.getNeonUserId())
                .setPageId(orig.getPageId())
                .setPageURL(orig.getPageURL())
                .setRefURL(orig.getRefURL())
                .setServerTime((float) (orig.getServerTime() / 1000.))
                .setTrackerAccountId(orig.getTrackerAccountId())
                .setTrackerType(orig.getTrackerType())
                .setUserAgent(orig.getUserAgent())
                .setSequenceId(sequenceId)
                .setThumbnailId(thumbnailId)
                .setVideoId(ExtractVideoId(thumbnailId.toString()))
                .setPageCoords(clickCoords)
                .setWindowCoords(
                    ((ImageClick) orig.getEventData()).getWindowCoords())
                .setIsClickInPlayer(false)
                .setIsRightClick(
                    clickCoords.getX() <= 0 && clickCoords.getY() <= 0).build();
        out.write("ImageClickHive", hiveEvent, NullWritable.get(),
            "ImageClickHive" + partitionPath);
        break;

      case VIDEO_CLICK:
        // We know if it is a right click only if the click coordinates are 0.
        hiveEvent =
            ImageClickHive
                .newBuilder()
                .setAgentInfo(orig.getAgentInfo())
                .setClientIP(orig.getClientIP())
                .setClientTime((float) (orig.getClientTime() / 1000.))
                .setIpGeoData(orig.getIpGeoData())
                .setNeonUserId(orig.getNeonUserId())
                .setPageId(orig.getPageId())
                .setPageURL(orig.getPageURL())
                .setRefURL(orig.getRefURL())
                .setServerTime((float) (orig.getServerTime() / 1000.))
                .setTrackerAccountId(orig.getTrackerAccountId())
                .setTrackerType(orig.getTrackerType())
                .setUserAgent(orig.getUserAgent())
                .setSequenceId(sequenceId)
                .setThumbnailId(
                    ((VideoClick) orig.getEventData()).getThumbnailId())
                .setVideoId(((VideoClick) orig.getEventData()).getVideoId())
                .setPageCoords(new Coords(-1f, -1f))
                .setWindowCoords(new Coords(-1f, -1f)).setIsClickInPlayer(true)
                .setIsRightClick(false).build();
        out.write("ImageClickHive", hiveEvent, NullWritable.get(),
            "ImageClickHive" + partitionPath);
        break;

      case AD_PLAY:
        // We know if it is a right click only if the click coordinates are 0.
        hiveEvent =
            AdPlayHive
                .newBuilder()
                .setAgentInfo(orig.getAgentInfo())
                .setClientIP(orig.getClientIP())
                .setClientTime((float) (orig.getClientTime() / 1000.))
                .setIpGeoData(orig.getIpGeoData())
                .setNeonUserId(orig.getNeonUserId())
                .setPageId(orig.getPageId())
                .setPageURL(orig.getPageURL())
                .setRefURL(orig.getRefURL())
                .setServerTime((float) (orig.getServerTime() / 1000.))
                .setTrackerAccountId(orig.getTrackerAccountId())
                .setTrackerType(orig.getTrackerType())
                .setUserAgent(orig.getUserAgent())
                .setSequenceId(sequenceId)
                .setVideoId(((AdPlay) orig.getEventData()).getVideoId())
                .setThumbnailId(((AdPlay) orig.getEventData()).getThumbnailId())
                .setPlayerId(((AdPlay) orig.getEventData()).getPlayerId())
                .setAutoplayDelta(
                    ((AdPlay) orig.getEventData()).getAutoplayDelta())
                .setPlayCount(((AdPlay) orig.getEventData()).getPlayCount())
                .build();
        out.write("AdPlayHive", hiveEvent, NullWritable.get(), "AdPlayHive"
            + partitionPath);
        break;

      case VIDEO_PLAY:
        // We know if it is a right click only if the click coordinates are 0.
        hiveEvent =
            VideoPlayHive
                .newBuilder()
                .setAgentInfo(orig.getAgentInfo())
                .setClientIP(orig.getClientIP())
                .setClientTime((float) (orig.getClientTime() / 1000.))
                .setIpGeoData(orig.getIpGeoData())
                .setNeonUserId(orig.getNeonUserId())
                .setPageId(orig.getPageId())
                .setPageURL(orig.getPageURL())
                .setRefURL(orig.getRefURL())
                .setServerTime((float) (orig.getServerTime() / 1000.))
                .setTrackerAccountId(orig.getTrackerAccountId())
                .setTrackerType(orig.getTrackerType())
                .setUserAgent(orig.getUserAgent())
                .setSequenceId(sequenceId)
                .setVideoId(((VideoPlay) orig.getEventData()).getVideoId())
                .setThumbnailId(
                    ((VideoPlay) orig.getEventData()).getThumbnailId())
                .setPlayerId(((VideoPlay) orig.getEventData()).getPlayerId())
                .setAutoplayDelta(
                    ((VideoPlay) orig.getEventData()).getAutoplayDelta())
                .setPlayCount(((VideoPlay) orig.getEventData()).getPlayCount())
                .setDidAdPlay(((VideoPlay) orig.getEventData()).getDidAdPlay())
                .build();
        out.write("VideoPlayHive", hiveEvent, NullWritable.get(),
            "VideoPlayHive" + partitionPath);
        break;

      default:
        context.getCounter("ReduceError", "InvalidEventType").increment(1);
      }
    }
  }

  /**
   * Extracts the external video id from a thumbnail id string
   * 
   * @param thumbnailId
   * @return The external video id
   */
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
    AvroMultipleOutputs.addNamedOutput(job, "ImageLoadHive",
        AvroKeyOutputFormat.class, ImageLoadHive.getClassSchema());
    AvroMultipleOutputs.addNamedOutput(job, "ImageVisibleHive",
        AvroKeyOutputFormat.class, ImageVisibleHive.getClassSchema());
    AvroMultipleOutputs.addNamedOutput(job, "ImageClickHive",
        AvroKeyOutputFormat.class, ImageClickHive.getClassSchema());
    AvroMultipleOutputs.addNamedOutput(job, "AdPlayHive",
        AvroKeyOutputFormat.class, AdPlayHive.getClassSchema());
    AvroMultipleOutputs.addNamedOutput(job, "VideoPlayHive",
        AvroKeyOutputFormat.class, VideoPlayHive.getClassSchema());

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
