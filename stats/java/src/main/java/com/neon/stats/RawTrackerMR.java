/**
 * 
 */
package com.neon.stats;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
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
  // The maximum time that a sequence can occur over (1 hours)
  private static final long MAX_SEQUENCE_TIME = 3600000;
  private static final String UNKNOWN_IP = "";

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
      // Fix any broken ip addresses
      TrackerEvent curEvent = key.datum();
      curEvent.setClientIP(NormalizeIpAddress(curEvent.getClientIP()));

      // Build the map key prefix
      String mapKey =
          curEvent.getTrackerAccountId().toString() + curEvent.getClientIP();

      String videoId;
      String editThumbId;

      try {

        switch (curEvent.getEventType()) {

        case IMAGES_VISIBLE:
          List<CharSequence> thumbnailids =
              ((ImagesVisible) key.datum().getEventData()).getThumbnailIds();
          for (CharSequence thumbnailId : thumbnailids) {
            // We don't copy the original event because it doesn't need to be.
            // The
            // new events are copied when they are written.
            editThumbId = NormalizeThumbnailId(thumbnailId);
            ImageVisible newEventData = new ImageVisible(editThumbId);
            TrackerEvent newEvent = key.datum();
            newEvent.setEventData(newEventData);
            newEvent.setEventType(EventType.IMAGE_VISIBLE);
            context.write(new Text(mapKey + ExtractVideoId(editThumbId)),
                new AvroValue<TrackerEvent>(newEvent));
          }
          break;

        case IMAGES_LOADED:
          List<ImageLoad> imageLoads =
              ((ImagesLoaded) key.datum().getEventData()).getImages();
          for (ImageLoad imageLoad : imageLoads) {
            // We don't copy the original event because it doesn't need to be.
            // The
            // new events are copied when they are written.
            editThumbId = NormalizeThumbnailId(imageLoad.getThumbnailId());
            imageLoad.setThumbnailId(editThumbId);
            TrackerEvent newEvent = key.datum();
            newEvent.setEventData(imageLoad);
            newEvent.setEventType(EventType.IMAGE_LOAD);
            context.write(new Text(mapKey + ExtractVideoId(editThumbId)),
                new AvroValue<TrackerEvent>(newEvent));
          }
          break;

        case IMAGE_CLICK:
          ImageClick imageClickData = (ImageClick) key.datum().getEventData();
          editThumbId = NormalizeThumbnailId(imageClickData.getThumbnailId());
          imageClickData.setThumbnailId(editThumbId);
          context.write(new Text(mapKey + ExtractVideoId(editThumbId)),
              new AvroValue<TrackerEvent>(key.datum()));
          break;

        case VIDEO_CLICK:
          VideoClick videoClickData = (VideoClick) key.datum().getEventData();
          videoClickData.setThumbnailId(NormalizeThumbnailId(videoClickData
              .getThumbnailId()));
          videoId = videoClickData.getVideoId().toString();
          context.write(new Text(mapKey + videoId),
              new AvroValue<TrackerEvent>(key.datum()));
          break;

        case VIDEO_PLAY:
          VideoPlay videoPlayData = (VideoPlay) key.datum().getEventData();
          videoPlayData.setThumbnailId(NormalizeThumbnailId(videoPlayData
              .getThumbnailId()));
          videoId = videoPlayData.getVideoId().toString();
          context.write(new Text(mapKey + videoId),
              new AvroValue<TrackerEvent>(key.datum()));
          break;

        case AD_PLAY:
          // The video id could be null in an ad play. For now don't try to
          // figure
          // out the associated video id
          // TODO(mdesnoyer): Figure out the video id in this case.
          AdPlay adPlayData = (AdPlay) key.datum().getEventData();
          adPlayData.setThumbnailId(NormalizeThumbnailId(adPlayData
              .getThumbnailId()));
          if (adPlayData.getVideoId() == null) {
            context.write(new Text(mapKey),
                new AvroValue<TrackerEvent>(key.datum()));
          } else {
            videoId = adPlayData.getVideoId().toString();
            context.write(new Text(mapKey + videoId),
                new AvroValue<TrackerEvent>(key.datum()));
          }
          break;

        case VIDEO_VIEW_PERCENTAGE:
          VideoViewPercentage videoVPData =
              (VideoViewPercentage) key.datum().getEventData();
          videoId = videoVPData.getVideoId().toString();
          context.write(new Text(mapKey + videoId),
              new AvroValue<TrackerEvent>(key.datum()));
          break;

        default:
          context.getCounter("MappingError", "InvalidEvent").increment(1);
        }

      } catch (ClassCastException e) {
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
        events.add(DeepCopyTrackerEvent(value.datum()));
      }
      context.setStatus("Starting sort");
      Collections.sort(events, new Comparator<TrackerEvent>() {
        public int compare(TrackerEvent a, TrackerEvent b) {
          int timeDiff = a.getClientTime().compareTo(b.getClientTime());
          if (timeDiff == 0) {
            // Force the event types to be clustered
            return a.getEventType().compareTo(b.getEventType());
          }
          return timeDiff;
        }
      });
      context.setStatus("Done sort");

      // Build up the list of events as they will be in their Hive tables
      LinkedList<Pair<TrackerEvent, Long>> seqEvents =
          new LinkedList<Pair<TrackerEvent, Long>>();
      long eventNum = 0;
      for (ListIterator<TrackerEvent> baseI = events.listIterator(); baseI
          .hasNext();) {
        if (++eventNum % 100 == 0) {
          context.setStatus("Processing event " + eventNum);
        }

        TrackerEvent curEvent = baseI.next();
        long curSequenceId = InitializeSequenceId(curEvent, context);

        // If the event is a duplicate, remove the older event
        ListIterator<Pair<TrackerEvent, Long>> revIter =
            seqEvents.listIterator(seqEvents.size());
        TrackerEvent oldEvent;
        while (revIter.hasPrevious()) {
          oldEvent = revIter.previous().getLeft();
          if (IsDuplicateTrackerEvent(oldEvent, curEvent)) {
            revIter.remove();
            context.getCounter("EventStats", "DuplicatesFound").increment(1);
            break;
          } else if ((curEvent.getServerTime() - oldEvent.getServerTime()) > MAX_SEQUENCE_TIME) {
            // Stop looking for duplicates that are too old
            break;
          }
        }

        if (!curEvent.getClientIP().equals(UNKNOWN_IP)) {
          BackfillSequenceId(curEvent, seqEvents, curSequenceId, context);
        }

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
            } else if (!curEvent.getClientIP().equals(UNKNOWN_IP)
                && (oldEvent.getEventType() == EventType.VIDEO_PLAY || oldEvent
                    .getEventType() == EventType.VIDEO_CLICK)) {
              // There is another video play from this user so we're done
              // looking
              break;
            } else if ((curEvent.getClientTime() - oldEvent.getClientTime()) > MAX_SEQUENCE_TIME) {
              // Stop looking because it's too old
              break;
            }
          }
          if (foundRealClick) {
            // There is a real click so we can ignore this video click
            context.getCounter("EventStats", "ExtraVideoClicks").increment(1);
            continue;
          }

        }

        // Add the current event to the ones to write out
        seqEvents.add(MutablePair.of(curEvent, curSequenceId));

      }

      // Now output the resulting events.
      context.setStatus("Outputing Events");
      for (Pair<TrackerEvent, Long> pair : seqEvents) {
        OutputEventToHive(pair.getLeft(), pair.getRight(), context);
      }

      // Finally output the sequence events
      context.setStatus("Outputting sequence events");
      OutputSequenceEvents(seqEvents, context);
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
      case VIDEO_VIEW_PERCENTAGE:
        videoPlayHash =
            ((VideoViewPercentage) event.getEventData()).getPlayCount()
                ^ ((VideoViewPercentage) event.getEventData()).getVideoId()
                    .hashCode();
        break;
      default:
        context.getCounter("ReduceError", "InvalidEventType").increment(1);
      }

      long retval =
          (((long) event.getPageId().hashCode()) << 32)
              | (((long) videoPlayHash) & 0xFFFFFFFFl);
      return retval;
    }

    private static void BackfillSequenceId(TrackerEvent curEvent,
        LinkedList<Pair<TrackerEvent, Long>> seqEvents, long curSequenceId,
        Context context) {
      Pair<TrackerEvent, Long> eventPair;
      if (curEvent.getEventType() == EventType.AD_PLAY
          || curEvent.getEventType() == EventType.VIDEO_PLAY) {
        boolean fromOtherPage =
            curEvent.getRefURL() != null && curEvent.getRefURL().length() > 0
                && IsFirstAutoplay(curEvent);
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

      case VIDEO_VIEW_PERCENTAGE:
        // For the video view percentage, they are the same if it's for the same
        // video id and play count since we only care about the max view
        // percentage.
        return a.getPageId().equals(b.getPageId())
            && ((VideoViewPercentage) a.getEventData()).getVideoId().equals(
                ((VideoViewPercentage) b.getEventData()).getVideoId())
            && ((VideoViewPercentage) a.getEventData()).getPlayCount().equals(
                ((VideoViewPercentage) b.getEventData()).getPlayCount());

      case IMAGE_CLICK:
      case VIDEO_CLICK:
      case VIDEO_PLAY:
      case AD_PLAY:
        // TODO(mdesnoyer): Maybe flag play & click events as duplicates if they
        // are too close in time instead of exactly the same time
        return a.getPageId().equals(b.getPageId())
            && a.getClientTime().equals(b.getClientTime());

      }

      return true;
    }

    /**
     * Returns true if the play event was the result of an autoplay
     */
    private static boolean IsAutoplay(TrackerEvent event) {
      if (event.getEventType() == EventType.VIDEO_PLAY) {
        VideoPlay data = (VideoPlay) event.getEventData();
        if (data.getIsAutoPlay() == null) {
          return data.getAutoplayDelta() == null
              || data.getAutoplayDelta() > 2000;
        } else {
          return data.getIsAutoPlay();
        }
      } else if (event.getEventType() == EventType.AD_PLAY) {
        AdPlay adData = (AdPlay) event.getEventData();
        if (adData.getIsAutoPlay() == null) {
          return adData.getAutoplayDelta() == null
              || adData.getAutoplayDelta() > 2000;
        } else {
          return adData.getIsAutoPlay();
        }
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
      Coords imageCoords;

      String partitionPath =
          GeneratePartitionPath(orig.getServerTime(),
              orig.getTrackerAccountId());

      // Do some null handling of common structures
      NmVers browser =
          orig.getAgentInfo() == null ? null : orig.getAgentInfo().getBrowser();
      CharSequence browserName = browser == null ? null : browser.getName();
      CharSequence browserVersion =
          browser == null ? null : browser.getVersion();

      NmVers os =
          orig.getAgentInfo() == null ? null : orig.getAgentInfo().getOs();
      CharSequence osName = os == null ? null : os.getName();
      CharSequence osVersion = os == null ? null : os.getVersion();

      switch (orig.getEventType()) {
      case IMAGE_LOAD:
        hiveEvent =
            ImageLoadHive
                .newBuilder()
                .setAgentInfoBrowserName(browserName)
                .setAgentInfoBrowserVersion(browserVersion)
                .setAgentInfoOsName(osName)
                .setAgentInfoOsVersion(osVersion)
                .setClientIP(orig.getClientIP())
                .setClientTime(orig.getClientTime() / 1000.)
                .setIpGeoDataCity(orig.getIpGeoData().getCity())
                .setIpGeoDataCountry(orig.getIpGeoData().getCountry())
                .setIpGeoDataRegion(orig.getIpGeoData().getRegion())
                .setIpGeoDataZip(orig.getIpGeoData().getZip())
                .setIpGeoDataLat(orig.getIpGeoData().getLat())
                .setIpGeoDataLon(orig.getIpGeoData().getLon())
                .setNeonUserId(orig.getNeonUserId())
                .setPageId(orig.getPageId())
                .setPageURL(orig.getPageURL())
                .setRefURL(orig.getRefURL())
                .setServerTime(orig.getServerTime() / 1000.)
                .setTrackerAccountId(orig.getTrackerAccountId())
                .setTrackerType(orig.getTrackerType())
                .setUserAgent(orig.getUserAgent())
                .setSequenceId(sequenceId)
                .setThumbnailId(
                    ((ImageLoad) orig.getEventData()).getThumbnailId())
                .setHeight(((ImageLoad) orig.getEventData()).getHeight())
                .setWidth(((ImageLoad) orig.getEventData()).getWidth()).build();
        out.write("ImageLoadHive", new AvroKey<ImageLoadHive>(
            (ImageLoadHive) hiveEvent), NullWritable.get(), "ImageLoadHive"
            + partitionPath + "ImageLoadHive");
        break;

      case IMAGE_VISIBLE:
        hiveEvent =
            ImageVisibleHive
                .newBuilder()
                .setAgentInfoBrowserName(browserName)
                .setAgentInfoBrowserVersion(browserVersion)
                .setAgentInfoOsName(osName)
                .setAgentInfoOsVersion(osVersion)
                .setClientIP(orig.getClientIP())
                .setClientTime(orig.getClientTime() / 1000.)
                .setIpGeoDataCity(orig.getIpGeoData().getCity())
                .setIpGeoDataCountry(orig.getIpGeoData().getCountry())
                .setIpGeoDataRegion(orig.getIpGeoData().getRegion())
                .setIpGeoDataZip(orig.getIpGeoData().getZip())
                .setIpGeoDataLat(orig.getIpGeoData().getLat())
                .setIpGeoDataLon(orig.getIpGeoData().getLon())
                .setNeonUserId(orig.getNeonUserId())
                .setPageId(orig.getPageId())
                .setPageURL(orig.getPageURL())
                .setRefURL(orig.getRefURL())
                .setServerTime(orig.getServerTime() / 1000.)
                .setTrackerAccountId(orig.getTrackerAccountId())
                .setTrackerType(orig.getTrackerType())
                .setUserAgent(orig.getUserAgent())
                .setSequenceId(sequenceId)
                .setThumbnailId(
                    ((ImageVisible) orig.getEventData()).getThumbnailId())
                .build();
        out.write("ImageVisibleHive", new AvroKey<ImageVisibleHive>(
            (ImageVisibleHive) hiveEvent), NullWritable.get(),
            "ImageVisibleHive" + partitionPath + "ImageVisibleHive");
        break;

      case IMAGE_CLICK:
        // We know if it is a right click only if the click coordinates are 0.
        clickCoords = ((ImageClick) orig.getEventData()).getPageCoords();
        imageCoords = ((ImageClick) orig.getEventData()).getImageCoords();
        CharSequence thumbnailId =
            ((ImageClick) orig.getEventData()).getThumbnailId();
        hiveEvent =
            ImageClickHive
                .newBuilder()
                .setAgentInfoBrowserName(browserName)
                .setAgentInfoBrowserVersion(browserVersion)
                .setAgentInfoOsName(osName)
                .setAgentInfoOsVersion(osVersion)
                .setClientIP(orig.getClientIP())
                .setClientTime(orig.getClientTime() / 1000.)
                .setIpGeoDataCity(orig.getIpGeoData().getCity())
                .setIpGeoDataCountry(orig.getIpGeoData().getCountry())
                .setIpGeoDataRegion(orig.getIpGeoData().getRegion())
                .setIpGeoDataZip(orig.getIpGeoData().getZip())
                .setIpGeoDataLat(orig.getIpGeoData().getLat())
                .setIpGeoDataLon(orig.getIpGeoData().getLon())
                .setNeonUserId(orig.getNeonUserId())
                .setPageId(orig.getPageId())
                .setPageURL(orig.getPageURL())
                .setRefURL(orig.getRefURL())
                .setServerTime(orig.getServerTime() / 1000.)
                .setTrackerAccountId(orig.getTrackerAccountId())
                .setTrackerType(orig.getTrackerType())
                .setUserAgent(orig.getUserAgent())
                .setSequenceId(sequenceId)
                .setThumbnailId(thumbnailId)
                .setVideoId(ExtractVideoId(thumbnailId.toString()))
                .setPageCoordsX(clickCoords.getX())
                .setPageCoordsY(clickCoords.getY())
                .setWindowCoordsX(
                    ((ImageClick) orig.getEventData()).getWindowCoords().getX())
                .setWindowCoordsY(
                    ((ImageClick) orig.getEventData()).getWindowCoords().getY())
                .setImageCoordsX(
                    imageCoords == null ? null : imageCoords.getX())
                .setImageCoordsY(
                    imageCoords == null ? null : imageCoords.getY())
                .setIsClickInPlayer(false)
                .setIsRightClick(
                    clickCoords.getX() <= 0 && clickCoords.getY() <= 0).build();
        out.write("ImageClickHive", new AvroKey<ImageClickHive>(
            (ImageClickHive) hiveEvent), NullWritable.get(), "ImageClickHive"
            + partitionPath + "ImageClickHive");
        break;

      case VIDEO_CLICK:
        // We know if it is a right click only if the click coordinates are 0.
        hiveEvent =
            ImageClickHive
                .newBuilder()
                .setAgentInfoBrowserName(browserName)
                .setAgentInfoBrowserVersion(browserVersion)
                .setAgentInfoOsName(osName)
                .setAgentInfoOsVersion(osVersion)
                .setClientIP(orig.getClientIP())
                .setClientTime(orig.getClientTime() / 1000.)
                .setIpGeoDataCity(orig.getIpGeoData().getCity())
                .setIpGeoDataCountry(orig.getIpGeoData().getCountry())
                .setIpGeoDataRegion(orig.getIpGeoData().getRegion())
                .setIpGeoDataZip(orig.getIpGeoData().getZip())
                .setIpGeoDataLat(orig.getIpGeoData().getLat())
                .setIpGeoDataLon(orig.getIpGeoData().getLon())
                .setNeonUserId(orig.getNeonUserId())
                .setPageId(orig.getPageId())
                .setPageURL(orig.getPageURL())
                .setRefURL(orig.getRefURL())
                .setServerTime(orig.getServerTime() / 1000.)
                .setTrackerAccountId(orig.getTrackerAccountId())
                .setTrackerType(orig.getTrackerType())
                .setUserAgent(orig.getUserAgent())
                .setSequenceId(sequenceId)
                .setThumbnailId(
                    ((VideoClick) orig.getEventData()).getThumbnailId())
                .setVideoId(((VideoClick) orig.getEventData()).getVideoId())
                .setPageCoordsX(-1f).setPageCoordsY(-1f).setWindowCoordsX(-1f)
                .setWindowCoordsY(-1f).setImageCoordsX(-1f)
                .setImageCoordsY(-1f).setIsClickInPlayer(true)
                .setIsRightClick(false).build();
        out.write("ImageClickHive", new AvroKey<ImageClickHive>(
            (ImageClickHive) hiveEvent), NullWritable.get(), "ImageClickHive"
            + partitionPath + "ImageClickHive");
        break;

      case AD_PLAY:
        // We know if it is a right click only if the click coordinates are 0.
        hiveEvent =
            AdPlayHive
                .newBuilder()
                .setAgentInfoBrowserName(browserName)
                .setAgentInfoBrowserVersion(browserVersion)
                .setAgentInfoOsName(osName)
                .setAgentInfoOsVersion(osVersion)
                .setClientIP(orig.getClientIP())
                .setClientTime(orig.getClientTime() / 1000.)
                .setIpGeoDataCity(orig.getIpGeoData().getCity())
                .setIpGeoDataCountry(orig.getIpGeoData().getCountry())
                .setIpGeoDataRegion(orig.getIpGeoData().getRegion())
                .setIpGeoDataZip(orig.getIpGeoData().getZip())
                .setIpGeoDataLat(orig.getIpGeoData().getLat())
                .setIpGeoDataLon(orig.getIpGeoData().getLon())
                .setNeonUserId(orig.getNeonUserId())
                .setPageId(orig.getPageId())
                .setPageURL(orig.getPageURL())
                .setRefURL(orig.getRefURL())
                .setServerTime(orig.getServerTime() / 1000.)
                .setTrackerAccountId(orig.getTrackerAccountId())
                .setTrackerType(orig.getTrackerType())
                .setUserAgent(orig.getUserAgent())
                .setSequenceId(sequenceId)
                .setVideoId(((AdPlay) orig.getEventData()).getVideoId())
                .setThumbnailId(((AdPlay) orig.getEventData()).getThumbnailId())
                .setPlayerId(((AdPlay) orig.getEventData()).getPlayerId())
                .setAutoplayDelta(
                    ((AdPlay) orig.getEventData()).getAutoplayDelta())
                .setIsAutoPlay(IsAutoplay(orig))
                .setPlayCount(((AdPlay) orig.getEventData()).getPlayCount())
                .build();
        out.write("AdPlayHive",
            new AvroKey<AdPlayHive>((AdPlayHive) hiveEvent),
            NullWritable.get(), "AdPlayHive" + partitionPath + "AdPlayHive");
        break;

      case VIDEO_PLAY:
        // We know if it is a right click only if the click coordinates are 0.
        hiveEvent =
            VideoPlayHive
                .newBuilder()
                .setAgentInfoBrowserName(browserName)
                .setAgentInfoBrowserVersion(browserVersion)
                .setAgentInfoOsName(osName)
                .setAgentInfoOsVersion(osVersion)
                .setClientIP(orig.getClientIP())
                .setClientTime(orig.getClientTime() / 1000.)
                .setIpGeoDataCity(orig.getIpGeoData().getCity())
                .setIpGeoDataCountry(orig.getIpGeoData().getCountry())
                .setIpGeoDataRegion(orig.getIpGeoData().getRegion())
                .setIpGeoDataZip(orig.getIpGeoData().getZip())
                .setIpGeoDataLat(orig.getIpGeoData().getLat())
                .setIpGeoDataLon(orig.getIpGeoData().getLon())
                .setNeonUserId(orig.getNeonUserId())
                .setPageId(orig.getPageId())
                .setPageURL(orig.getPageURL())
                .setRefURL(orig.getRefURL())
                .setServerTime(orig.getServerTime() / 1000.)
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
                .setIsAutoPlay(IsAutoplay(orig))
                .setPlayCount(((VideoPlay) orig.getEventData()).getPlayCount())
                .setDidAdPlay(((VideoPlay) orig.getEventData()).getDidAdPlay())
                .build();
        out.write("VideoPlayHive", new AvroKey<VideoPlayHive>(
            (VideoPlayHive) hiveEvent), NullWritable.get(), "VideoPlayHive"
            + partitionPath + "VideoPlayHive");
        break;

      case VIDEO_VIEW_PERCENTAGE:
        hiveEvent =
            VideoViewPercentageHive
                .newBuilder()
                .setAgentInfoBrowserName(browserName)
                .setAgentInfoBrowserVersion(browserVersion)
                .setAgentInfoOsName(osName)
                .setAgentInfoOsVersion(osVersion)
                .setClientIP(orig.getClientIP())
                .setClientTime(orig.getClientTime() / 1000.)
                .setIpGeoDataCity(orig.getIpGeoData().getCity())
                .setIpGeoDataCountry(orig.getIpGeoData().getCountry())
                .setIpGeoDataRegion(orig.getIpGeoData().getRegion())
                .setIpGeoDataZip(orig.getIpGeoData().getZip())
                .setIpGeoDataLat(orig.getIpGeoData().getLat())
                .setIpGeoDataLon(orig.getIpGeoData().getLon())
                .setNeonUserId(orig.getNeonUserId())
                .setPageId(orig.getPageId())
                .setPageURL(orig.getPageURL())
                .setRefURL(orig.getRefURL())
                .setServerTime(orig.getServerTime() / 1000.)
                .setTrackerAccountId(orig.getTrackerAccountId())
                .setTrackerType(orig.getTrackerType())
                .setUserAgent(orig.getUserAgent())
                .setSequenceId(sequenceId)
                .setVideoId(
                    ((VideoViewPercentage) orig.getEventData()).getVideoId())
                .setPlayCount(
                    ((VideoViewPercentage) orig.getEventData()).getPlayCount())
                .setPercent(
                    ((VideoViewPercentage) orig.getEventData()).getPercent())
                .build();
        out.write("VideoViewPercentageHive",
            new AvroKey<VideoViewPercentageHive>(
                (VideoViewPercentageHive) hiveEvent), NullWritable.get(),
            "VideoViewPercentageHive" + partitionPath
                + "VideoViewPercentageHive");
        break;

      default:
        context.getCounter("ReduceError", "InvalidEventType").increment(1);
      }
    }

    private void OutputSequenceEvents(
        LinkedList<Pair<TrackerEvent, Long>> events, Context context)
        throws IOException, InterruptedException {

      // First sort the events by their sequence id
      Collections.sort(events, new Comparator<Pair<TrackerEvent, Long>>() {
        public int compare(Pair<TrackerEvent, Long> a,
            Pair<TrackerEvent, Long> b) {
          int seqDiff = a.getRight().compareTo(b.getRight());

          if (seqDiff == 0) {
            // Force the event types to be clustered
            return a.getLeft().getClientTime()
                .compareTo(b.getLeft().getClientTime());
          }
          return seqDiff;
        }
      });

      // Now walk through the events and generate an event for each sequence id.
      TrackerEvent imLoad = null;
      TrackerEvent imVis = null;
      TrackerEvent imClick = null;
      TrackerEvent adPlay = null;
      TrackerEvent videoClick = null;
      TrackerEvent videoPlay = null;
      TrackerEvent videoViewPercentage = null;
      Long curSequenceId = null;
      for (Pair<TrackerEvent, Long> pair : events) {
        if (curSequenceId == null) {
          curSequenceId = pair.getRight();
        } else if (!curSequenceId.equals(pair.getRight())) {
          // It's the start of a new sequence, so output the last sequence
          OutputSequence(curSequenceId, imLoad, imVis, imClick, adPlay,
              videoClick, videoPlay, videoViewPercentage);
          imLoad = null;
          imVis = null;
          imClick = null;
          adPlay = null;
          videoClick = null;
          videoPlay = null;
          videoViewPercentage = null;
          curSequenceId = pair.getRight();
        }

        // Assign the event based on its type
        switch (pair.getLeft().getEventType()) {
        case IMAGE_LOAD:
          imLoad = pair.getLeft();
          break;
        case IMAGE_VISIBLE:
          imVis = pair.getLeft();
          break;
        case IMAGE_CLICK:
          imClick = pair.getLeft();
          break;
        case AD_PLAY:
          adPlay = pair.getLeft();
          break;
        case VIDEO_CLICK:
          videoClick = pair.getLeft();
          break;
        case VIDEO_PLAY:
          videoPlay = pair.getLeft();
          break;
        case VIDEO_VIEW_PERCENTAGE:
          videoViewPercentage = pair.getLeft();
          break;
        }
      }

      if (curSequenceId != null) {
        OutputSequence(curSequenceId, imLoad, imVis, imClick, adPlay,
            videoClick, videoPlay, videoViewPercentage);
      }
    }

    private void OutputSequence(Long sequenceId, TrackerEvent imLoad,
        TrackerEvent imVis, TrackerEvent imClick, TrackerEvent adPlay,
        TrackerEvent videoClick, TrackerEvent videoPlay,
        TrackerEvent videoViewPercentage) throws IOException,
        InterruptedException {
      long lastServerTime = 0;
      CharSequence trackerAccountId = "";
      CharSequence firstRefURL = null;
      CharSequence thumbnailId = null;
      EventSequenceHive.Builder builder =
          EventSequenceHive.newBuilder().setSequenceId(sequenceId);

      if (imLoad != null) {
        lastServerTime = imLoad.getServerTime();
        trackerAccountId = imLoad.getTrackerAccountId();
        firstRefURL = imLoad.getRefURL();
        thumbnailId = ((ImageLoad) imLoad.getEventData()).getThumbnailId();
        BuildCommonSequenceFields(builder, imLoad)
            .setHeight(((ImageLoad) imLoad.getEventData()).getHeight())
            .setWidth(((ImageLoad) imLoad.getEventData()).getWidth())
            .setImLoadClientTime(imLoad.getClientTime() / 1000.)
            .setImLoadServerTime(imLoad.getServerTime() / 1000.)
            .setImLoadPageURL(imLoad.getPageURL())
            .setImLoadPageId(imLoad.getPageId());
      }

      if (imVis != null) {
        lastServerTime = imVis.getServerTime();
        trackerAccountId = imVis.getTrackerAccountId();
        firstRefURL = firstRefURL == null ? imVis.getRefURL() : firstRefURL;
        thumbnailId =
            thumbnailId == null ? ((ImageVisible) imVis.getEventData())
                .getThumbnailId() : thumbnailId;
        BuildCommonSequenceFields(builder, imVis)
            .setImVisClientTime(imVis.getClientTime() / 1000.)
            .setImVisServerTime(imVis.getServerTime() / 1000.)
            .setImLoadPageURL(imVis.getPageURL())
            .setImVisPageId(imVis.getPageId());
      }

      if (imClick != null) {
        lastServerTime = imClick.getServerTime();
        trackerAccountId = imClick.getTrackerAccountId();
        firstRefURL = firstRefURL == null ? imClick.getRefURL() : firstRefURL;
        thumbnailId =
            thumbnailId == null ? ((ImageClick) imClick.getEventData())
                .getThumbnailId() : thumbnailId;
        Coords clickCoords =
            ((ImageClick) imClick.getEventData()).getPageCoords();
        Coords imageCoords =
            ((ImageClick) imClick.getEventData()).getImageCoords();
        BuildCommonSequenceFields(builder, imClick)
            .setPageCoordsX(clickCoords.getX())
            .setPageCoordsY(clickCoords.getY())
            .setWindowCoordsX(
                ((ImageClick) imClick.getEventData()).getWindowCoords().getX())
            .setWindowCoordsY(
                ((ImageClick) imClick.getEventData()).getWindowCoords().getY())
            .setImageCoordsX(imageCoords == null ? null : imageCoords.getX())
            .setImageCoordsY(imageCoords == null ? null : imageCoords.getY())
            .setIsRightClick(clickCoords.getX() <= 0 && clickCoords.getY() <= 0)
            .setIsClickInPlayer(false)
            .setImClickClientTime(imClick.getClientTime() / 1000.)
            .setImClickServerTime(imClick.getServerTime() / 1000.)
            .setImClickPageURL(imClick.getPageURL())
            .setImClickPageId(imClick.getPageId());
      }

      if (adPlay != null) {
        lastServerTime = adPlay.getServerTime();
        trackerAccountId = adPlay.getTrackerAccountId();
        firstRefURL = firstRefURL == null ? adPlay.getRefURL() : firstRefURL;
        thumbnailId =
            thumbnailId == null ? ((AdPlay) adPlay.getEventData())
                .getThumbnailId() : thumbnailId;
        BuildCommonSequenceFields(builder, adPlay)
            .setPlayerId(((AdPlay) adPlay.getEventData()).getPlayerId())
            .setVideoId(((AdPlay) adPlay.getEventData()).getVideoId())
            .setAutoplayDelta(
                ((AdPlay) adPlay.getEventData()).getAutoplayDelta())
            .setIsAutoPlay(IsAutoplay(adPlay))
            .setPlayCount(((AdPlay) adPlay.getEventData()).getPlayCount())
            .setAdPlayClientTime(adPlay.getClientTime() / 1000.)
            .setAdPlayServerTime(adPlay.getServerTime() / 1000.)
            .setVideoPageURL(adPlay.getPageURL())
            .setAdPlayPageId(adPlay.getPageId());
      }

      if (videoClick != null) {
        lastServerTime = videoClick.getServerTime();
        trackerAccountId = videoClick.getTrackerAccountId();
        firstRefURL =
            firstRefURL == null ? videoClick.getRefURL() : firstRefURL;
        thumbnailId =
            thumbnailId == null ? ((VideoClick) videoClick.getEventData())
                .getThumbnailId() : thumbnailId;
        BuildCommonSequenceFields(builder, videoClick)
            .setPlayerId(((VideoClick) videoClick.getEventData()).getPlayerId())
            .setVideoId(((VideoClick) videoClick.getEventData()).getVideoId())
            .setIsClickInPlayer(true).setIsRightClick(false)
            .setImClickClientTime(videoClick.getClientTime() / 1000.)
            .setImClickServerTime(videoClick.getServerTime() / 1000.)
            .setImClickPageURL(videoClick.getPageURL())
            .setImClickPageId(videoClick.getPageId());
      }

      if (videoPlay != null) {
        lastServerTime = videoPlay.getServerTime();
        trackerAccountId = videoPlay.getTrackerAccountId();
        firstRefURL = firstRefURL == null ? videoPlay.getRefURL() : firstRefURL;
        thumbnailId =
            thumbnailId == null ? ((VideoPlay) videoPlay.getEventData())
                .getThumbnailId() : thumbnailId;
        BuildCommonSequenceFields(builder, videoPlay)
            .setPlayerId(((VideoPlay) videoPlay.getEventData()).getPlayerId())
            .setVideoId(((VideoPlay) videoPlay.getEventData()).getVideoId())
            .setAutoplayDelta(
                ((VideoPlay) videoPlay.getEventData()).getAutoplayDelta())
            .setIsAutoPlay(IsAutoplay(videoPlay))
            .setPlayCount(((VideoPlay) videoPlay.getEventData()).getPlayCount())
            .setVideoPlayClientTime(videoPlay.getClientTime() / 1000.)
            .setVideoPlayServerTime(videoPlay.getServerTime() / 1000.)
            .setVideoPageURL(videoPlay.getPageURL())
            .setVideoPlayPageId(videoPlay.getPageId());
      }

      if (videoViewPercentage != null) {
        lastServerTime = videoViewPercentage.getServerTime();
        trackerAccountId = videoViewPercentage.getTrackerAccountId();
        firstRefURL =
            firstRefURL == null ? videoViewPercentage.getRefURL() : firstRefURL;
        BuildCommonSequenceFields(builder, videoViewPercentage)
            .setVideoId(
                ((VideoViewPercentage) videoViewPercentage.getEventData())
                    .getVideoId())
            .setPlayCount(
                ((VideoViewPercentage) videoViewPercentage.getEventData())
                    .getPlayCount())
            .setVideoViewPercent(
                ((VideoViewPercentage) videoViewPercentage.getEventData())
                    .getPercent());
      }

      builder.setRefURL(firstRefURL).setThumbnailId(thumbnailId)
          .setServerTime(lastServerTime / 1000.);

      out.write(
          "EventSequenceHive",
          new AvroKey<EventSequenceHive>(builder.build()),
          NullWritable.get(),
          "EventSequenceHive"
              + GeneratePartitionPath(lastServerTime, trackerAccountId)
              + "EventSequenceHive");
    }

    private static EventSequenceHive.Builder BuildCommonSequenceFields(
        EventSequenceHive.Builder builder, TrackerEvent event) {
      // Do some null handling of common structures
      NmVers browser =
          event.getAgentInfo() == null ? null : event.getAgentInfo()
              .getBrowser();
      CharSequence browserName = browser == null ? null : browser.getName();
      CharSequence browserVersion =
          browser == null ? null : browser.getVersion();

      NmVers os =
          event.getAgentInfo() == null ? null : event.getAgentInfo().getOs();
      CharSequence osName = os == null ? null : os.getName();
      CharSequence osVersion = os == null ? null : os.getVersion();

      builder.setTrackerAccountId(event.getTrackerAccountId())
          .setTrackerType(event.getTrackerType())
          .setClientIP(event.getClientIP())
          .setNeonUserId(event.getNeonUserId())
          .setUserAgent(event.getUserAgent())
          .setAgentInfoBrowserName(browserName)
          .setAgentInfoBrowserVersion(browserVersion)
          .setAgentInfoOsName(osName).setAgentInfoOsVersion(osVersion)
          .setIpGeoDataCity(event.getIpGeoData().getCity())
          .setIpGeoDataCountry(event.getIpGeoData().getCountry())
          .setIpGeoDataRegion(event.getIpGeoData().getRegion())
          .setIpGeoDataZip(event.getIpGeoData().getZip())
          .setIpGeoDataLat(event.getIpGeoData().getLat())
          .setIpGeoDataLon(event.getIpGeoData().getLon());

      return builder;
    }

    /**
     * Creates a path for this partition in Hive
     * 
     * @param epoch
     *          - The timestamp in milliseconds since epoch
     * @param trackerAccountId
     *          - The tracker account id
     * @return A string to differentiate the partition
     */
    private static String GeneratePartitionPath(long epoch,
        CharSequence trackerAccountId) {
      // Create the output path that partitions based on time and the account id
      SimpleDateFormat timestampFormat = new SimpleDateFormat("YYYY-MM-dd");
      String timestamp = timestampFormat.format(new Date(epoch));

      // TODO(mdesnoyer): There is a bug in Hive (fixed in 0.12) such that it
      // cannot read partitioned Avro tables, so, we need to create one single,
      // gigantic partition now. When this is ported into Impala, we
      // can partition this data to make it more efficient, but for now, dump it
      // in a single folder.
      // return "/tai=" + trackerAccountId + "/ts=" + timestamp + "/";
      return "/";
    }
  }

  /**
   * Converts the thumbnail id into its normalize form of
   * <api_key>_<video_id>_<thumbnailid>. Sometimes, these can be separated by
   * dashes instead of underscores.
   * 
   * @param thumbnailId
   * @return The normalized thumbnail id
   */
  private static String NormalizeThumbnailId(CharSequence thumbnailId) {
    if (thumbnailId == null) {
      return null;
    }
    Pattern dashRe = Pattern.compile("^[0-9a-zA-Z]+\\-[0-9a-zA-Z~\\.]+\\-[0-9a-zA-Z]+$");
    if (dashRe.matcher(thumbnailId.toString()).matches()) {
      return thumbnailId.toString().replaceAll("\\-", "_");
    } else {
      return thumbnailId.toString();
    }
  }

  /**
   * Normalizes the ip address to deal with quirks.
   * 
   * For now, we just remove those addresses that actually internal ones and set
   * it to unknown (empty string)
   * 
   * @param ipAddress
   * @return The normalized ip address
   */
  private static CharSequence NormalizeIpAddress(CharSequence ipAddress) {
    String ipString = ipAddress.toString();

    // Internal addresses are "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"
    if (ipString.startsWith("10.") || ipString.startsWith("172.16.")
        || ipString.startsWith("192.168.")) {
      return UNKNOWN_IP;
    }

    return ipAddress;
  }

  /**
   * Extracts the external video id from a thumbnail id string
   * 
   * @param thumbnailId
   * @return The external video id
   */
  private static String ExtractVideoId(String thumbnailId) {
    if (thumbnailId == null) {
      return "";
    }
    String[] split = thumbnailId.split("[_\\-]");
    if (split.length != 3) {
      // Invalid video id
      return "";
    }
    return split[1];
  }

  /**
   * Does a deep copy of an Avro Record the hard way (by serializing and
   * deserializing)
   * 
   * This would ideally not be necessary, but Record.newBuilder(other).build()
   * seems to have casting problems sometimes.
   * 
   * @param other
   *          The record to copy
   * @return A deep copy of the record
   * @throws IOException
   */
  private static TrackerEvent DeepCopyTrackerEvent(TrackerEvent other)
      throws IOException {
    if (other == null) {
      return null;
    }

    DatumReader<TrackerEvent> datumReader =
        new SpecificDatumReader<TrackerEvent>(TrackerEvent.class);
    DatumWriter<TrackerEvent> datumWriter =
        new SpecificDatumWriter<TrackerEvent>(TrackerEvent.class);
    ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
    BinaryEncoder binaryEncoder =
        EncoderFactory.get().directBinaryEncoder(byteOutStream, null);

    datumWriter.write(other, binaryEncoder);
    BinaryDecoder binaryDecoder =
        DecoderFactory.get().binaryDecoder(byteOutStream.toByteArray(), null);
    return datumReader.read(null, binaryDecoder);
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
    
    Configuration conf = getConf();

    Job job = Job.getInstance(conf);
    job.setJarByClass(RawTrackerMR.class);
    job.setJobName("Raw Tracker Data Cleaning");

    FileInputFormat.setInputDirRecursive(job, true);
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // Define the mapper
    job.setMapperClass(MapToUserEventStream.class);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    AvroJob.setInputKeySchema(job, TrackerEvent.getClassSchema());
    AvroJob.setMapOutputValueSchema(job, TrackerEvent.getClassSchema());
    job.getConfiguration().set(AvroJob.CONF_OUTPUT_CODEC, "snappy");
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
    AvroMultipleOutputs.addNamedOutput(job, "VideoViewPercentageHive",
        AvroKeyOutputFormat.class, VideoViewPercentageHive.getClassSchema());
    AvroMultipleOutputs.addNamedOutput(job, "EventSequenceHive",
        AvroKeyOutputFormat.class, EventSequenceHive.getClassSchema());

    job.submit();
    // job.waitForCompletion(true);
    JobStatus jobStatus = job.getStatus();
    System.out.println("Job ID: " + jobStatus.getJobID());
    System.out.println("Tracking URL: " + jobStatus.getTrackingUrl());

    return 0;
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
