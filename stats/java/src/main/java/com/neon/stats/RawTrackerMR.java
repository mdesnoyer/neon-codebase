/**
 * 
 */
package com.neon.stats;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

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
          continue;
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

      // TODO(mdesnoyer): There is a bug in Hive (fixed in 0.12) such that it
      // cannot read partitioned Avro tables, so, we need to create one single,
      // gigantic partition now. When this is ported into Elastic MapReduce, we
      // can partition this data to make it more efficient, but for now, dump it
      // in a single folder.
      // String partitionPath =
      // "/tai=" + orig.getTrackerAccountId() + "/ts=" + timestamp + "/";
      String partitionPath = "/";

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
                .setClientTime((float) (orig.getClientTime() / 1000.))
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
                .setServerTime((float) (orig.getServerTime() / 1000.))
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
                .setClientTime((float) (orig.getClientTime() / 1000.))
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
                .setServerTime((float) (orig.getServerTime() / 1000.))
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
                .setClientTime((float) (orig.getClientTime() / 1000.))
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
                .setServerTime((float) (orig.getServerTime() / 1000.))
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
                .setClientTime((float) (orig.getClientTime() / 1000.))
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
                .setServerTime((float) (orig.getServerTime() / 1000.))
                .setTrackerAccountId(orig.getTrackerAccountId())
                .setTrackerType(orig.getTrackerType())
                .setUserAgent(orig.getUserAgent())
                .setSequenceId(sequenceId)
                .setThumbnailId(
                    ((VideoClick) orig.getEventData()).getThumbnailId())
                .setVideoId(((VideoClick) orig.getEventData()).getVideoId())
                .setPageCoordsX(-1f).setPageCoordsY(-1f).setWindowCoordsX(-1f)
                .setWindowCoordsY(-1f).setIsClickInPlayer(true)
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
                .setClientTime((float) (orig.getClientTime() / 1000.))
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
                .setClientTime((float) (orig.getClientTime() / 1000.))
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
        out.write("VideoPlayHive", new AvroKey<VideoPlayHive>(
            (VideoPlayHive) hiveEvent), NullWritable.get(), "VideoPlayHive"
            + partitionPath + "VideoPlayHive");
        break;

      default:
        context.getCounter("ReduceError", "InvalidEventType").increment(1);
      }
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
    return thumbnailId.toString().replaceAll("\\-", "_");
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

    Job job = Job.getInstance(getConf());
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

    job.submit();
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
