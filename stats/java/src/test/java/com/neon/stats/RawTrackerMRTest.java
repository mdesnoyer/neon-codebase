package com.neon.stats;

import java.io.IOException;
import java.util.*;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.mockito.Mockito.*;

import com.neon.stats.RawTrackerMR.*;
import com.neon.Tracker.*;

public class RawTrackerMRTest {
  MapDriver<AvroKey<TrackerEvent>, NullWritable, Text, AvroValue<TrackerEvent>> mapDriver;
  ReduceDriver<Text, AvroValue<TrackerEvent>, WritableComparable<Object>, NullWritable> reduceDriver;
  MapReduceDriver<AvroKey<TrackerEvent>, NullWritable, Text, AvroValue<TrackerEvent>, WritableComparable<Object>, NullWritable> mapReduceDriver;

  AvroMultipleOutputs outputCollector;
  Map<Long, HashSet<SpecificRecordBase>> capturedSequences;

  @Before
  public void setUp() throws IOException {
    // We need to manually mock out the multiple outputs because it's not in
    // MRUnit 1.0.0. It will be in 1.1.0, but that hasn't been released yet.
    outputCollector = mock(AvroMultipleOutputs.class);

    MapToUserEventStream mapper = new MapToUserEventStream();
    CleanUserStreamReducer reducer =
        new CleanUserStreamReducer(outputCollector);
    mapDriver = MapDriver.newMapDriver(mapper);
    Configuration conf = mapDriver.getConfiguration();
    AvroSerialization.addToConfiguration(conf);
    AvroSerialization.setKeyWriterSchema(conf, TrackerEvent.getClassSchema());
    AvroSerialization.setKeyReaderSchema(conf, TrackerEvent.getClassSchema());
    AvroSerialization.setValueWriterSchema(conf, TrackerEvent.getClassSchema());
    AvroSerialization.setValueReaderSchema(conf, TrackerEvent.getClassSchema());
    Job job = Job.getInstance(conf);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(AvroValue.class);
    AvroJob.setInputKeySchema(job, TrackerEvent.getClassSchema());
    AvroJob.setMapOutputValueSchema(job, TrackerEvent.getClassSchema());

    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    Configuration reduceConf = reduceDriver.getConfiguration();
    AvroSerialization.addToConfiguration(reduceConf);
    AvroSerialization.setKeyWriterSchema(reduceConf,
        TrackerEvent.getClassSchema());
    AvroSerialization.setKeyReaderSchema(reduceConf,
        TrackerEvent.getClassSchema());
    AvroSerialization.setValueWriterSchema(reduceConf,
        TrackerEvent.getClassSchema());
    AvroSerialization.setValueReaderSchema(reduceConf,
        TrackerEvent.getClassSchema());

    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    Configuration integConf = mapReduceDriver.getConfiguration();
    AvroSerialization.addToConfiguration(integConf);
    AvroSerialization.setKeyWriterSchema(integConf,
        TrackerEvent.getClassSchema());
    AvroSerialization.setKeyReaderSchema(integConf,
        TrackerEvent.getClassSchema());
    AvroSerialization.setValueWriterSchema(integConf,
        TrackerEvent.getClassSchema());
    AvroSerialization.setValueReaderSchema(integConf,
        TrackerEvent.getClassSchema());

  }

  /**
   * Checks that the given event sequence was found
   * 
   * @param expectedEvents
   *          List of Hive events to expect to output from a given run. useful
   *          in reducer tests
   */
  protected void VerifySequence(List<SpecificRecordBase> expectedEventList) {

    HashSet<SpecificRecordBase> expectedEvents =
        new HashSet<SpecificRecordBase>(expectedEventList);

    String missingInfo = "";

    for (Map.Entry<Long, HashSet<SpecificRecordBase>> entry : capturedSequences
        .entrySet()) {
      Long sequenceId = entry.getKey();
      HashSet<SpecificRecordBase> eventsSeen = entry.getValue();

      if (eventsSeen.size() != expectedEvents.size()) {
        // Not this sequence, it's a different size
        continue;
      }

      boolean eventMissing = false;
      for (SpecificRecordBase expectedEvent : expectedEvents) {
        // Add the sequence id to the expected event
        if (expectedEvent instanceof ImageLoadHive) {
          ((ImageLoadHive) expectedEvent).setSequenceId(sequenceId);
        } else if (expectedEvent instanceof ImageVisibleHive) {
          ((ImageVisibleHive) expectedEvent).setSequenceId(sequenceId);
        } else if (expectedEvent instanceof ImageClickHive) {
          ((ImageClickHive) expectedEvent).setSequenceId(sequenceId);
        } else if (expectedEvent instanceof AdPlayHive) {
          ((AdPlayHive) expectedEvent).setSequenceId(sequenceId);
        } else if (expectedEvent instanceof VideoPlayHive) {
          ((VideoPlayHive) expectedEvent).setSequenceId(sequenceId);
        } else if (expectedEvent instanceof VideoViewPercentageHive) {
          ((VideoViewPercentageHive) expectedEvent).setSequenceId(sequenceId);
        } else if (expectedEvent instanceof EventSequenceHive) {
          ((EventSequenceHive) expectedEvent).setSequenceId(sequenceId);
        }

        // See if the event was captured
        if (!eventsSeen.contains(expectedEvent)) {
          eventMissing = true;

          missingInfo = "For event type " + expectedEvent.getClass().getName();

          // Try to find the event that it might have been
          for (SpecificRecordBase eventSeen : eventsSeen) {
            if (eventSeen.getClass() == expectedEvent.getClass()) {
              for (Schema.Field field : expectedEvent.getSchema().getFields()) {
                Object expectedValue = expectedEvent.get(field.pos());
                if (expectedValue instanceof String) {
                  // String get turned to utf8 by avro
                  expectedValue = new Utf8((String) expectedValue);
                }
                Object foundValue = eventSeen.get(field.pos());
                if (expectedValue == null ? foundValue != null : !expectedValue
                    .equals(foundValue)) {
                  missingInfo +=
                      " Field " + field.name() + " is different. Expected: "
                          + expectedValue + " Actual: " + foundValue;
                }
              }
            }
          }
        }
      }

      if (!eventMissing) {
        // We found the sequence
        return;
      }
    }

    fail("The expected sequence was not found. It might be because: "
        + missingInfo + ". Sequence: " + expectedEventList.toString());
  }

  /**
   * Captures all the events
   * 
   * @param eventNames
   *          - List of event types to capture
   * @throws InterruptedException
   * @throws IOException
   */
  protected void CaptureEvents() throws IOException, InterruptedException {
    ArgumentCaptor<AvroKey> hiveEventCaptor =
        ArgumentCaptor.forClass(AvroKey.class);
    ArgumentCaptor<String> outputPathCaptor =
        ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> nameCaptor = ArgumentCaptor.forClass(String.class);

    verify(outputCollector, atLeastOnce()).write(nameCaptor.capture(),
        hiveEventCaptor.capture(), eq(NullWritable.get()),
        outputPathCaptor.capture());

    capturedSequences = new HashMap<Long, HashSet<SpecificRecordBase>>();
    List<AvroKey> hiveEvents = hiveEventCaptor.getAllValues();
    List<String> outputPaths = outputPathCaptor.getAllValues();
    assertEquals(hiveEvents.size(), outputPaths.size());
    Long sequenceId = null;
    for (int i = 0; i < hiveEvents.size(); ++i) {
      AvroKey curEvent = hiveEvents.get(i);
      if (curEvent.datum() instanceof ImageLoadHive) {
        sequenceId = ((ImageLoadHive) curEvent.datum()).getSequenceId();
        assertEquals(outputPaths.get(i), "ImageLoadHive/ImageLoadHive");

      } else if (curEvent.datum() instanceof ImageVisibleHive) {
        sequenceId = ((ImageVisibleHive) curEvent.datum()).getSequenceId();
        assertEquals(outputPaths.get(i), "ImageVisibleHive/ImageVisibleHive");

      } else if (curEvent.datum() instanceof ImageClickHive) {
        sequenceId = ((ImageClickHive) curEvent.datum()).getSequenceId();
        assertEquals(outputPaths.get(i), "ImageClickHive/ImageClickHive");

      } else if (curEvent.datum() instanceof AdPlayHive) {
        sequenceId = ((AdPlayHive) curEvent.datum()).getSequenceId();
        assertEquals(outputPaths.get(i), "AdPlayHive/AdPlayHive");

      } else if (curEvent.datum() instanceof VideoPlayHive) {
        sequenceId = ((VideoPlayHive) curEvent.datum()).getSequenceId();
        assertEquals(outputPaths.get(i), "VideoPlayHive/VideoPlayHive");

      } else if (curEvent.datum() instanceof VideoViewPercentageHive) {
        sequenceId =
            ((VideoViewPercentageHive) curEvent.datum()).getSequenceId();
        assertEquals(outputPaths.get(i),
            "VideoViewPercentageHive/VideoViewPercentageHive");

      } else if (curEvent.datum() instanceof EventSequenceHive) {
        sequenceId = ((EventSequenceHive) curEvent.datum()).getSequenceId();
        assertEquals(outputPaths.get(i), "EventSequenceHive/EventSequenceHive");
      } else {
        break;
      }

      // Add the event to the sequence list
      HashSet<SpecificRecordBase> seqList = capturedSequences.get(sequenceId);
      if (seqList == null) {
        seqList = new HashSet<SpecificRecordBase>();
        capturedSequences.put(sequenceId, seqList);
      }
      seqList.add((SpecificRecordBase) curEvent.datum());
    }
  }

  protected TrackerEvent.Builder MakeBasicTrackerEvent() {
    return TrackerEvent.newBuilder(new TrackerEvent("pageid1", "tai1",
        TrackerType.BRIGHTCOVE, "http://go.com", "http://ref.com",
        1400697546000l, 1400000000000l, "56.45.41.124", "", "agent1", null,
        new GeoData("USA", null, "CA", null, 34.556f, 120.45f),
        EventType.IMAGES_VISIBLE, null));
  }

  protected TrackerEvent.Builder MakeBasicImageLoad() {
    return MakeBasicTrackerEvent()
        .setEventType(EventType.IMAGE_LOAD)
        .setEventData(new ImageLoad("acct1_vid1_tid1", 480, 640))
        .setAgentInfo(
            new AgentInfo(new NmVers("Windows", "7"), new NmVers(
                "Internet Explorer", "10.01")));
  }

  protected ImageLoadHive.Builder MakeBasicImageLoadHive() {
    return ImageLoadHive.newBuilder()
        .setAgentInfoBrowserName("Internet Explorer")
        .setAgentInfoBrowserVersion("10.01").setAgentInfoOsName("Windows")
        .setAgentInfoOsVersion("7").setClientIP("56.45.41.124")
        .setClientTime(1400000000.).setIpGeoDataCity(null)
        .setIpGeoDataCountry("USA").setIpGeoDataRegion("CA")
        .setIpGeoDataZip(null).setIpGeoDataLat(34.556f)
        .setIpGeoDataLon(120.45f).setNeonUserId("").setPageId("pageid1")
        .setPageURL("http://go.com").setRefURL("http://ref.com")
        .setServerTime(1400697546.).setTrackerType(TrackerType.BRIGHTCOVE)
        .setTrackerAccountId("tai1").setUserAgent("agent1").setHeight(480)
        .setWidth(640).setThumbnailId("acct1_vid1_tid1").setSequenceId(0);
  }

  protected TrackerEvent.Builder MakeBasicImageVisible() {
    return MakeBasicTrackerEvent().setEventType(EventType.IMAGE_VISIBLE)
        .setEventData(new ImageVisible("acct1_vid1_tid1"));
  }

  protected ImageVisibleHive.Builder MakeBasicImageVisibleHive() {
    return ImageVisibleHive.newBuilder().setAgentInfoBrowserName(null)
        .setAgentInfoBrowserVersion(null).setAgentInfoOsName(null)
        .setAgentInfoOsVersion(null).setClientIP("56.45.41.124")
        .setClientTime(1400000000.).setIpGeoDataCity(null)
        .setIpGeoDataCountry("USA").setIpGeoDataRegion("CA")
        .setIpGeoDataZip(null).setIpGeoDataLat(34.556f)
        .setIpGeoDataLon(120.45f).setNeonUserId("").setPageId("pageid1")
        .setPageURL("http://go.com").setRefURL("http://ref.com")
        .setTrackerAccountId("tai1").setServerTime(1400697546.)
        .setTrackerType(TrackerType.BRIGHTCOVE).setUserAgent("agent1")
        .setThumbnailId("acct1_vid1_tid1").setSequenceId(0);
  }

  protected TrackerEvent.Builder MakeBasicImageClick() {
    return MakeBasicTrackerEvent().setEventType(EventType.IMAGE_CLICK)
        .setEventData(
            new ImageClick(true, "acct1_vid1_tid1", new Coords(100f, 200f),
                new Coords(150f, 250f), new Coords(3f, 4f)));
  }

  protected ImageClickHive.Builder MakeBasicImageClickHive() {
    return ImageClickHive.newBuilder().setAgentInfoBrowserName(null)
        .setAgentInfoBrowserVersion(null).setAgentInfoOsName(null)
        .setAgentInfoOsVersion(null).setClientIP("56.45.41.124")
        .setClientTime(1400000000.).setIpGeoDataCity(null)
        .setIpGeoDataCountry("USA").setIpGeoDataRegion("CA")
        .setIpGeoDataZip(null).setIpGeoDataLat(34.556f)
        .setIpGeoDataLon(120.45f).setNeonUserId("").setPageId("pageid1")
        .setPageURL("http://go.com").setRefURL("http://ref.com")
        .setTrackerAccountId("tai1").setServerTime(1400697546.)
        .setTrackerType(TrackerType.BRIGHTCOVE).setUserAgent("agent1")
        .setThumbnailId("acct1_vid1_tid1").setVideoId("vid1")
        .setPageCoordsX(100f).setPageCoordsY(200f).setWindowCoordsX(150f)
        .setWindowCoordsY(250f).setImageCoordsX(3f).setImageCoordsY(4f)
        .setIsClickInPlayer(false).setIsRightClick(false).setSequenceId(0);
  }

  protected TrackerEvent.Builder MakeBasicAdPlay() {
    return MakeBasicTrackerEvent().setEventType(EventType.AD_PLAY)
        .setEventData(
            new AdPlay(true, "vid1", "player2", "acct1_vid1_tid1", null, null,
                1));
  }

  protected AdPlayHive.Builder MakeBasicAdPlayHive() {
    return AdPlayHive.newBuilder().setAgentInfoBrowserName(null)
        .setAgentInfoBrowserVersion(null).setAgentInfoOsName(null)
        .setAgentInfoOsVersion(null).setClientIP("56.45.41.124")
        .setClientTime(1400000000.).setIpGeoDataCity(null)
        .setIpGeoDataCountry("USA").setIpGeoDataRegion("CA")
        .setIpGeoDataZip(null).setIpGeoDataLat(34.556f)
        .setIpGeoDataLon(120.45f).setNeonUserId("").setPageId("pageid1")
        .setPageURL("http://go.com").setRefURL("http://ref.com")
        .setTrackerAccountId("tai1").setServerTime(1400697546.)
        .setTrackerType(TrackerType.BRIGHTCOVE).setUserAgent("agent1")
        .setThumbnailId("acct1_vid1_tid1").setVideoId("vid1")
        .setPlayerId("player2").setAutoplayDelta(null).setPlayCount(1)
        .setSequenceId(0);
  }

  protected TrackerEvent.Builder MakeBasicVideoClick() {
    return MakeBasicTrackerEvent().setEventType(EventType.VIDEO_CLICK)
        .setEventData(
            new VideoClick(true, "vid1", "player2", "acct1_vid1_tid1"));
  }

  protected ImageClickHive.Builder MakeBasicVideoClickHive() {
    return ImageClickHive.newBuilder().setAgentInfoBrowserName(null)
        .setAgentInfoBrowserVersion(null).setAgentInfoOsName(null)
        .setAgentInfoOsVersion(null).setClientIP("56.45.41.124")
        .setClientTime(1400000000.).setIpGeoDataCity(null)
        .setIpGeoDataCountry("USA").setIpGeoDataRegion("CA")
        .setIpGeoDataZip(null).setIpGeoDataLat(34.556f)
        .setIpGeoDataLon(120.45f).setNeonUserId("").setPageId("pageid1")
        .setPageURL("http://go.com").setRefURL("http://ref.com")
        .setTrackerAccountId("tai1").setServerTime(1400697546.)
        .setTrackerType(TrackerType.BRIGHTCOVE).setUserAgent("agent1")
        .setThumbnailId("acct1_vid1_tid1").setVideoId("vid1")
        .setPageCoordsX(-1f).setPageCoordsY(-1f).setWindowCoordsX(-1f)
        .setWindowCoordsY(-1f).setImageCoordsX(-1f).setImageCoordsY(-1f)
        .setIsClickInPlayer(true).setIsRightClick(false).setSequenceId(0);
  }

  protected TrackerEvent.Builder MakeBasicVideoPlay() {
    return MakeBasicTrackerEvent().setEventType(EventType.VIDEO_PLAY)
        .setEventData(
            new VideoPlay(true, "vid1", "player2", "acct1_vid1_tid1", true,
                null, null, 1));
  }

  protected VideoPlayHive.Builder MakeBasicVideoPlayHive() {
    return VideoPlayHive.newBuilder().setAgentInfoBrowserName(null)
        .setAgentInfoBrowserVersion(null).setAgentInfoOsName(null)
        .setAgentInfoOsVersion(null).setClientIP("56.45.41.124")
        .setClientTime(1400000000.).setIpGeoDataCity(null)
        .setIpGeoDataCountry("USA").setIpGeoDataRegion("CA")
        .setIpGeoDataZip(null).setIpGeoDataLat(34.556f)
        .setIpGeoDataLon(120.45f).setNeonUserId("").setPageId("pageid1")
        .setPageURL("http://go.com").setRefURL("http://ref.com")
        .setTrackerAccountId("tai1").setServerTime(1400697546.)
        .setTrackerType(TrackerType.BRIGHTCOVE).setUserAgent("agent1")
        .setThumbnailId("acct1_vid1_tid1").setVideoId("vid1")
        .setPlayerId("player2").setAutoplayDelta(null).setPlayCount(1)
        .setDidAdPlay(true).setSequenceId(0);
  }

  protected TrackerEvent.Builder MakeBasicVideoViewPercentage() {
    return MakeBasicTrackerEvent()
        .setEventType(EventType.VIDEO_VIEW_PERCENTAGE).setEventData(
            new VideoViewPercentage(true, "vid1", 1, 34.6f));
  }

  protected VideoViewPercentageHive.Builder MakeBasicVideoViewPercentageHive() {
    return VideoViewPercentageHive.newBuilder().setAgentInfoBrowserName(null)
        .setAgentInfoBrowserVersion(null).setAgentInfoOsName(null)
        .setAgentInfoOsVersion(null).setClientIP("56.45.41.124")
        .setClientTime(1400000000.).setIpGeoDataCity(null)
        .setIpGeoDataCountry("USA").setIpGeoDataRegion("CA")
        .setIpGeoDataZip(null).setIpGeoDataLat(34.556f)
        .setIpGeoDataLon(120.45f).setNeonUserId("").setPageId("pageid1")
        .setPageURL("http://go.com").setRefURL("http://ref.com")
        .setTrackerAccountId("tai1").setServerTime(1400697546.)
        .setTrackerType(TrackerType.BRIGHTCOVE).setUserAgent("agent1")
        .setVideoId("vid1").setPlayCount(1).setPercent(34.6f).setSequenceId(0);
  }

  protected EventSequenceHive.Builder MakeBasicEventSequenceHive() {
    return EventSequenceHive.newBuilder().setAgentInfoBrowserName(null)
        .setAgentInfoBrowserVersion(null).setAgentInfoOsName(null)
        .setAgentInfoOsVersion(null).setClientIP("56.45.41.124")
        .setIpGeoDataCity(null).setIpGeoDataCountry("USA")
        .setIpGeoDataRegion("CA").setIpGeoDataZip(null)
        .setIpGeoDataLat(34.556f).setIpGeoDataLon(120.45f).setNeonUserId("")
        .setRefURL("http://ref.com").setTrackerAccountId("tai1")
        .setServerTime(1400697546.).setTrackerType(TrackerType.BRIGHTCOVE)
        .setUserAgent("agent1").setSequenceId(0);
  }

  @Test
  public void testImagesVisibleMapping() throws IOException {
    TrackerEvent.Builder baseEvent = MakeBasicTrackerEvent();

    ImagesVisible inputEventData =
        new ImagesVisible(true, Arrays.asList((CharSequence) "acct1_vid1_tid1",
            "acct1_vid2_tid12", "acct-vid31-tid0"));
    mapDriver.withInput(
        new AvroKey<TrackerEvent>(baseEvent
            .setEventType(EventType.IMAGES_VISIBLE)
            .setEventData(inputEventData).build()), NullWritable.get());

    mapDriver.withOutput(
        new Text("tai156.45.41.124vid1"),
        new AvroValue<TrackerEvent>(baseEvent
            .setEventType(EventType.IMAGE_VISIBLE)
            .setEventData(new ImageVisible("acct1_vid1_tid1")).build()));
    mapDriver.withOutput(
        new Text("tai156.45.41.124vid2"),
        new AvroValue<TrackerEvent>(baseEvent
            .setEventType(EventType.IMAGE_VISIBLE)
            .setEventData(new ImageVisible("acct1_vid2_tid12")).build()));
    mapDriver.withOutput(
        new Text("tai156.45.41.124vid31"),
        new AvroValue<TrackerEvent>(baseEvent
            .setEventType(EventType.IMAGE_VISIBLE)
            .setEventData(new ImageVisible("acct_vid31_tid0")).build()));
    mapDriver.runTest();
  }

  @Test
  public void testImagesLoadedMapping() throws IOException {
    TrackerEvent.Builder baseEvent = MakeBasicTrackerEvent();

    List<ImageLoad> loadList = new Vector<ImageLoad>();
    loadList.add(new ImageLoad("acct1_vid1_tid1", 56, 48));
    loadList.add(new ImageLoad("acct1-vid3-tid46", 480, 640));

    mapDriver.withInput(
        new AvroKey<TrackerEvent>(baseEvent
            .setEventType(EventType.IMAGES_LOADED)
            .setEventData(new ImagesLoaded(true, loadList)).build()),
        NullWritable.get());

    mapDriver.withOutput(
        new Text("tai156.45.41.124vid1"),
        new AvroValue<TrackerEvent>(baseEvent
            .setEventType(EventType.IMAGE_LOAD)
            .setEventData(new ImageLoad("acct1_vid1_tid1", 56, 48)).build()));
    mapDriver
        .withOutput(
            new Text("tai156.45.41.124vid3"),
            new AvroValue<TrackerEvent>(baseEvent
                .setEventType(EventType.IMAGE_LOAD)
                .setEventData(new ImageLoad("acct1_vid3_tid46", 480, 640))
                .build()));
    mapDriver.runTest();
  }

  @Test
  public void testImageClickMapping() throws IOException {
    TrackerEvent inputEvent =
        new TrackerEvent("pageid1", "tai1", TrackerType.BRIGHTCOVE,
            "http://go.com", "http://ref.com", 6498474l, 6549136l,
            "56.45.41.124", "", "agent1", null, new GeoData("USA", "San Fran",
                "CA", "94132", 34.556f, 120.45f), EventType.IMAGE_CLICK,
            new ImageClick(true, "acct1_vid2_tid3", new Coords(56f, 78.3f),
                new Coords(78f, 69f), new Coords(9f, 12f)));

    mapDriver.withInput(new AvroKey<TrackerEvent>(inputEvent),
        NullWritable.get());

    mapDriver.withOutput(new Text("tai156.45.41.124vid2"),
        new AvroValue<TrackerEvent>(inputEvent));

    mapDriver.runTest();
  }

  @Test
  public void testVideoClickPPGMappingRealThumbId() throws IOException {
    TrackerEvent inputEvent =
        TrackerEvent
            .newBuilder()
            .setEventType(EventType.VIDEO_CLICK)
            .setIpGeoData(new GeoData(null, null, null, null, null, null))
            .setPageId("lgFQCAy0PT5szYSr")
            .setNeonUserId("")
            .setTrackerAccountId("1483115066")
            .setServerTime(1412111219989l)
            .setClientTime(1412109680684l)
            .setPageURL("http://www.post-gazette.com/")
            .setRefURL("https://www.google.com/")
            .setTrackerType(TrackerType.BRIGHTCOVE)
            .setUserAgent(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36")
            .setClientIP("199.47.77.98")
            .setAgentInfo(
                AgentInfo.newBuilder()
                    .setBrowser(new NmVers("Chrome", "35.0.1916.114"))
                    .setOs(new NmVers("Linux", null)).build())
            .setEventData(
                new VideoClick(
                    true,
                    "3811926729001",
                    "1395884386001",
                    "6d3d519b15600c372a1f6735711d956e-3811926729001-7652d5c345f87dbfe517527c21045cc8"))
            .build();
    mapDriver.withInput(new AvroKey<TrackerEvent>(inputEvent),
        NullWritable.get());

    mapDriver
        .withOutput(
            new Text("1483115066199.47.77.983811926729001"),
            new AvroValue<TrackerEvent>(
                TrackerEvent
                    .newBuilder(inputEvent)
                    .setEventData(
                        new VideoClick(
                            true,
                            "3811926729001",
                            "1395884386001",
                            "6d3d519b15600c372a1f6735711d956e_3811926729001_7652d5c345f87dbfe517527c21045cc8"))
                    .build()));

    mapDriver.runTest();
  }

  @Test
  public void testVideoClickMapping() throws IOException {
    TrackerEvent inputEvent =
        MakeBasicTrackerEvent().setEventType(EventType.VIDEO_CLICK)
            .setEventData(new VideoClick(true, "vid2", "player2", null))
            .build();

    mapDriver.withInput(new AvroKey<TrackerEvent>(inputEvent),
        NullWritable.get());

    mapDriver.withOutput(new Text("tai156.45.41.124vid2"),
        new AvroValue<TrackerEvent>(inputEvent));

    mapDriver.runTest();
  }

  @Test
  public void testInternalIPMapping() throws IOException {
    TrackerEvent.Builder eventBuilder =
        MakeBasicTrackerEvent().setEventType(EventType.VIDEO_CLICK)
            .setEventData(new VideoClick(true, "vid2", "player2", null))
            .setClientIP("");

    mapDriver.withInput(
        new AvroKey<TrackerEvent>(eventBuilder.setClientIP("10.5.216.15")
            .setPageId("p0").build()), NullWritable.get());
    mapDriver.withInput(
        new AvroKey<TrackerEvent>(eventBuilder.setClientIP("172.16.64.15")
            .setPageId("p1").build()), NullWritable.get());
    mapDriver.withInput(
        new AvroKey<TrackerEvent>(eventBuilder.setClientIP("172.160.64.15")
            .setPageId("p2").build()), NullWritable.get());
    mapDriver.withInput(
        new AvroKey<TrackerEvent>(eventBuilder.setClientIP("192.168.67.34")
            .setPageId("p3").build()), NullWritable.get());

    mapDriver.withOutput(new Text("tai1vid2"), new AvroValue<TrackerEvent>(
        eventBuilder.setPageId("p0").setClientIP("").build()));
    mapDriver.withOutput(new Text("tai1vid2"), new AvroValue<TrackerEvent>(
        eventBuilder.setPageId("p1").setClientIP("").build()));
    mapDriver.withOutput(
        new Text("tai1172.160.64.15vid2"),
        new AvroValue<TrackerEvent>(eventBuilder.setPageId("p2")
            .setClientIP("172.160.64.15").build()));
    mapDriver.withOutput(new Text("tai1vid2"), new AvroValue<TrackerEvent>(
        eventBuilder.setPageId("p3").setClientIP("").build()));

    mapDriver.runTest();
  }

  @Test
  public void testVideoPlayMapping() throws IOException {
    TrackerEvent inputEvent =
        MakeBasicTrackerEvent()
            .setEventType(EventType.VIDEO_PLAY)
            .setEventData(
                new VideoPlay(true, "vid2", "player2", null, true, 600, null, 1))
            .build();

    mapDriver.withInput(new AvroKey<TrackerEvent>(inputEvent),
        NullWritable.get());

    mapDriver.withOutput(new Text("tai156.45.41.124vid2"),
        new AvroValue<TrackerEvent>(inputEvent));

    mapDriver.runTest();
  }

  @Test
  public void testAdPlayMapping() throws IOException {
    TrackerEvent inputEvent =
        MakeBasicTrackerEvent()
            .setEventType(EventType.AD_PLAY)
            .setEventData(
                new AdPlay(true, "vid2", "player2", null, null, null, 2))
            .build();

    mapDriver.withInput(new AvroKey<TrackerEvent>(inputEvent),
        NullWritable.get());

    mapDriver.withOutput(new Text("tai156.45.41.124vid2"),
        new AvroValue<TrackerEvent>(inputEvent));

    mapDriver.runTest();
  }

  @Test
  public void testVideoViewPercentageMapping() throws IOException {
    TrackerEvent inputEvent =
        MakeBasicTrackerEvent().setEventType(EventType.VIDEO_VIEW_PERCENTAGE)
            .setEventData(new VideoViewPercentage(true, "vid2", 3, 94.1f))
            .build();

    mapDriver.withInput(new AvroKey<TrackerEvent>(inputEvent),
        NullWritable.get());

    mapDriver.withOutput(new Text("tai156.45.41.124vid2"),
        new AvroValue<TrackerEvent>(inputEvent));

    mapDriver.runTest();
  }

  @Test
  public void testReductionClickOnDifferentPageThanVideo() throws IOException,
      InterruptedException {
    List<AvroValue<TrackerEvent>> values =
        new ArrayList<AvroValue<TrackerEvent>>();
    values.add(new AvroValue<TrackerEvent>(MakeBasicImageLoad().build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicImageVisible()
        .setClientTime(1400000000100l).build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicImageClick().setClientTime(
        1400000000200l).build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicImageClick().setClientTime(
        1400000000200l).build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicAdPlay()
        .setClientTime(1400000000500l).setPageId("vidpage")
        .setRefURL("http://go.com").build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicVideoClick()
        .setClientTime(1400000000600l).setPageId("vidpage")
        .setRefURL("http://go.com").build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicVideoPlay()
        .setClientTime(1400000000700l).setPageId("vidpage")
        .setRefURL("http://go.com").build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicVideoViewPercentage()
        .setClientTime(1400000000800l).setPageId("vidpage")
        .setRefURL("http://go.com").build()));

    // We have to look at the outputs in the mock because MRUnit doesn't handle
    // MultipleOutputs
    reduceDriver.withInput(new Text("tai156.45.41.124vid1"), values).run();

    CaptureEvents();

    VerifySequence(Arrays.asList(
        MakeBasicImageLoadHive().build(),
        MakeBasicImageVisibleHive().setClientTime(1400000000.1).build(),
        MakeBasicImageClickHive().setClientTime(1400000000.2).build(),
        MakeBasicAdPlayHive().setClientTime(1400000000.5).setPageId("vidpage")
            .setRefURL("http://go.com").setIsAutoPlay(true).build(),
        MakeBasicVideoPlayHive().setClientTime(1400000000.7)
            .setPageId("vidpage").setIsAutoPlay(true)
            .setRefURL("http://go.com").build(),
        MakeBasicVideoViewPercentageHive().setClientTime(1400000000.8)
            .setPageId("vidpage").setRefURL("http://go.com").build(),
        MakeBasicEventSequenceHive().setImVisClientTime(1400000000.1)
            .setImClickClientTime(1400000000.2)
            .setAdPlayClientTime(1400000000.5)
            .setVideoPlayClientTime(1400000000.7)
            .setImLoadClientTime(1400000000.).setVideoPageURL("http://go.com")
            .setImClickPageURL("http://go.com")
            .setImLoadPageURL("http://go.com").setImLoadServerTime(1400697546.)
            .setImVisServerTime(1400697546.).setImClickServerTime(1400697546.)
            .setAdPlayServerTime(1400697546.)
            .setVideoPlayServerTime(1400697546.).setServerTime(1400697546.)
            .setThumbnailId("acct1_vid1_tid1").setVideoId("vid1")
            .setPlayerId("player2").setAutoplayDelta(null).setPlayCount(1)
            .setPageCoordsX(100f).setPageCoordsY(200f).setWindowCoordsX(150f)
            .setWindowCoordsY(250f).setImageCoordsX(3f).setImageCoordsY(4f)
            .setIsClickInPlayer(false).setIsRightClick(false).setHeight(480)
            .setWidth(640).setImLoadPageId("pageid1").setImVisPageId("pageid1")
            .setImClickPageId("pageid1").setAdPlayPageId("vidpage")
            .setVideoPlayPageId("vidpage").setVideoViewPercent(34.6f)
            .setIsAutoPlay(true).build()));
  }

  @Test
  public void testReductionVideoClickSinglePlayer() throws IOException,
      InterruptedException {
    // This is a case where the player is a single player where the user can
    // just click play.
    List<AvroValue<TrackerEvent>> values =
        new ArrayList<AvroValue<TrackerEvent>>();
    values.add(new AvroValue<TrackerEvent>(MakeBasicImageLoad().build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicImageVisible()
        .setClientTime(1400000000100l).build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicVideoClick().setClientTime(
        1400000000600l).build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicVideoClick().setClientTime(
        1400000000650l).build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicVideoPlay()
        .setClientTime(1400000000700l)
        .setEventData(
            new VideoPlay(true, "vid1", "player2", "acct1_vid1_tid1", true,
                200, null, 1)).build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicVideoViewPercentage()
        .setClientTime(1400000000800l).build()));

    // We have to look at the outputs in the mock because MRUnit doesn't handle
    // MultipleOutputs
    reduceDriver.withInput(new Text("tai156.45.41.124vid1"), values).run();

    CaptureEvents();

    VerifySequence(Arrays.asList(
        MakeBasicImageLoadHive().build(),
        MakeBasicImageVisibleHive().setClientTime(1400000000.1).build(),
        MakeBasicVideoClickHive().setClientTime(1400000000.65).build(),
        MakeBasicVideoPlayHive().setClientTime(1400000000.7)
            .setAutoplayDelta(200).setIsAutoPlay(false).build(),
        MakeBasicVideoViewPercentageHive().setClientTime(1400000000.8).build(),
        MakeBasicEventSequenceHive().setImLoadClientTime(1400000000.)
            .setImVisClientTime(1400000000.1)
            .setImClickClientTime(1400000000.65)
            .setVideoPlayClientTime(1400000000.7)
            .setVideoPageURL("http://go.com")
            .setImClickPageURL("http://go.com")
            .setImLoadPageURL("http://go.com").setImLoadServerTime(1400697546.)
            .setImVisServerTime(1400697546.).setImClickServerTime(1400697546.)
            .setVideoPlayServerTime(1400697546.).setServerTime(1400697546.)
            .setThumbnailId("acct1_vid1_tid1").setVideoId("vid1")
            .setPlayerId("player2").setAutoplayDelta(200).setIsAutoPlay(false)
            .setPlayCount(1).setIsClickInPlayer(true).setIsRightClick(false)
            .setHeight(480).setWidth(640).setImLoadPageId("pageid1")
            .setImVisPageId("pageid1").setImClickPageId("pageid1")
            .setVideoPlayPageId("pageid1").setVideoViewPercent(34.6f).build()));
  }

  @Test
  public void testMultipleVideoViewPercentage() throws IOException,
      InterruptedException {
    // This is a case where a video plays and we have multiple view percentage
    // events arrive. We only want to keep track of the maximum one.
    List<AvroValue<TrackerEvent>> values =
        new ArrayList<AvroValue<TrackerEvent>>();
    values.add(new AvroValue<TrackerEvent>(MakeBasicVideoPlay().setClientTime(
        1400000000700l).build()));
    values
        .add(new AvroValue<TrackerEvent>(MakeBasicVideoViewPercentage()
            .setClientTime(1400000000900l)
            .setEventData(new VideoViewPercentage(true, "vid1", 1, 56.4f))
            .build()));
    values
        .add(new AvroValue<TrackerEvent>(MakeBasicVideoViewPercentage()
            .setClientTime(1400000000800l)
            .setEventData(new VideoViewPercentage(true, "vid1", 1, 23.1f))
            .build()));

    // We have to look at the outputs in the mock because MRUnit doesn't handle
    // MultipleOutputs
    reduceDriver.withInput(new Text("tai156.45.41.124vid1"), values).run();

    CaptureEvents();

    // Make sure that only one video view percentage hive entry is output
    assertEquals(capturedSequences.size(), 1);
    assertEquals(capturedSequences.values().iterator().next().size(), 3);

    VerifySequence(Arrays.asList(
        MakeBasicVideoPlayHive().setClientTime(1400000000.7)
            .setIsAutoPlay(true).build(), MakeBasicVideoViewPercentageHive()
            .setClientTime(1400000000.9).setPercent(56.4f).build(),
        MakeBasicEventSequenceHive().setVideoPlayClientTime(1400000000.7)
            .setVideoPlayServerTime(1400697546.).setServerTime(1400697546.)
            .setThumbnailId("acct1_vid1_tid1").setVideoId("vid1")
            .setVideoPageURL("http://go.com").setPlayerId("player2")
            .setAutoplayDelta(null).setIsAutoPlay(true).setPlayCount(1)
            .setVideoPlayPageId("pageid1").setVideoViewPercent(56.4f).build()));
  }

  @Test
  public void testClickFollowedByAutoplay() throws IOException,
      InterruptedException {
    // This is a case where the player is a single page and a click does the
    // first video play and then an autoplay causes the second
    List<AvroValue<TrackerEvent>> values =
        new ArrayList<AvroValue<TrackerEvent>>();
    values.add(new AvroValue<TrackerEvent>(MakeBasicImageLoad().build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicImageVisible()
        .setClientTime(1400000000100l).build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicVideoClick().setClientTime(
        1400000000600l).build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicVideoPlay()
        .setClientTime(1400000000700l)
        .setEventData(
            new VideoPlay(true, "vid1", "player2", "acct1_vid1_tid1", true,
                null, false, 1)).build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicVideoClick().setClientTime(
        1400000010600l).build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicVideoPlay()
        .setClientTime(1400000010700l)
        .setEventData(
            new VideoPlay(true, "vid1", "player2", "acct1_vid1_tid1", true,
                null, true, 2)).build()));

    reduceDriver.withInput(new Text("tai156.45.41.124vid1"), values).run();

    CaptureEvents();

    VerifySequence(Arrays
        .asList(
            MakeBasicVideoPlayHive().setClientTime(1400000010.7)
                .setAutoplayDelta(null).setPlayCount(2).setIsAutoPlay(true)
                .build(),
            MakeBasicEventSequenceHive().setVideoPlayClientTime(1400000010.7)
                .setVideoPageURL("http://go.com")
                .setVideoPlayServerTime(1400697546.).setServerTime(1400697546.)
                .setThumbnailId("acct1_vid1_tid1").setVideoId("vid1")
                .setPlayerId("player2").setIsAutoPlay(true).setPlayCount(2)
                .setVideoPlayPageId("pageid1").build()));
  }

  @Test
  public void testViewOnOnePageButClickToPlayOnVideoPage() throws IOException,
      InterruptedException {
    // The case where the user saw the image on one page, but got to the video
    // page by clicking a different video. Then they clicked to play this video
    // later.
    List<AvroValue<TrackerEvent>> values =
        new ArrayList<AvroValue<TrackerEvent>>();
    // The events on the first page
    values.add(new AvroValue<TrackerEvent>(MakeBasicImageLoad().build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicImageVisible()
        .setClientTime(1400000000100l).build()));

    // The events on the second page
    values.add(new AvroValue<TrackerEvent>(MakeBasicImageLoad()
        .setPageId("vidpage").setRefURL("http://go.com")
        .setClientTime(1400000010000l).build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicImageVisible()
        .setPageId("vidpage").setRefURL("http://go.com")
        .setClientTime(1400000010100l).build()));
    values.add(new AvroValue<TrackerEvent>(
        MakeBasicAdPlay()
            .setClientTime(1400000010500l)
            .setPageId("vidpage")
            .setRefURL("http://go.com")
            .setEventData(
                new AdPlay(true, "vid1", "player2", "acct1_vid1_tid1", 200,
                    null, 3)).build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicVideoClick()
        .setClientTime(1400000010600l).setPageId("vidpage")
        .setRefURL("http://go.com").build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicVideoPlay()
        .setClientTime(1400000010700l)
        .setPageId("vidpage")
        .setRefURL("http://go.com")
        .setEventData(
            new VideoPlay(true, "vid1", "player2", "acct1_vid1_tid1", true,
                200, null, 3)).build()));

    reduceDriver.withInput(new Text("tai156.45.41.124vid1"), values).run();

    CaptureEvents();

    // Check the sequence on page one, which is just a load and visible
    VerifySequence(Arrays.asList(MakeBasicImageLoadHive().build(),
        MakeBasicImageVisibleHive().setClientTime(1400000000.1).build(),
        MakeBasicEventSequenceHive().setImVisClientTime(1400000000.1)
            .setImLoadClientTime(1400000000.).setImLoadPageURL("http://go.com")
            .setImLoadServerTime(1400697546.).setImVisServerTime(1400697546.)
            .setServerTime(1400697546.).setThumbnailId("acct1_vid1_tid1")
            .setImLoadPageId("pageid1").setImVisPageId("pageid1").setWidth(640)
            .setHeight(480).build()));

    // Check the sequence on page 2
    VerifySequence(Arrays
        .asList(
            MakeBasicImageLoadHive().setClientTime(1400000010.0)
                .setPageId("vidpage").setRefURL("http://go.com").build(),
            MakeBasicImageVisibleHive().setClientTime(1400000010.1)
                .setPageId("vidpage").setRefURL("http://go.com").build(),
            MakeBasicVideoClickHive().setClientTime(1400000010.6)
                .setPageId("vidpage").setRefURL("http://go.com").build(),
            MakeBasicAdPlayHive().setClientTime(1400000010.5)
                .setPageId("vidpage").setRefURL("http://go.com")
                .setAutoplayDelta(200).setPlayCount(3).setIsAutoPlay(false)
                .build(),
            MakeBasicVideoPlayHive().setClientTime(1400000010.7)
                .setPageId("vidpage").setRefURL("http://go.com")
                .setAutoplayDelta(200).setPlayCount(3).setIsAutoPlay(false)
                .build(),
            MakeBasicEventSequenceHive().setImVisClientTime(1400000010.1)
                .setImClickClientTime(1400000010.6)
                .setAdPlayClientTime(1400000010.5)
                .setVideoPlayClientTime(1400000010.7)
                .setImLoadClientTime(1400000010.)
                .setVideoPageURL("http://go.com")
                .setImClickPageURL("http://go.com")
                .setImLoadPageURL("http://go.com")
                .setImLoadServerTime(1400697546.)
                .setImVisServerTime(1400697546.)
                .setImClickServerTime(1400697546.)
                .setAdPlayServerTime(1400697546.).setRefURL("http://go.com")
                .setVideoPlayServerTime(1400697546.).setServerTime(1400697546.)
                .setThumbnailId("acct1_vid1_tid1").setVideoId("vid1")
                .setPlayerId("player2").setAutoplayDelta(200)
                .setIsAutoPlay(false).setPlayCount(3).setIsClickInPlayer(true)
                .setIsRightClick(false).setHeight(480).setWidth(640)
                .setImLoadPageId("vidpage").setImVisPageId("vidpage")
                .setImClickPageId("vidpage").setAdPlayPageId("vidpage")
                .setVideoPlayPageId("vidpage").build()));
  }

  @Test
  public void testLoadSameImageOnMultiplePages() throws IOException,
      InterruptedException {
    List<AvroValue<TrackerEvent>> values =
        new ArrayList<AvroValue<TrackerEvent>>();
    // Page 1
    values.add(new AvroValue<TrackerEvent>(MakeBasicImageLoad().setPageId(
        "pageid1").build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicImageVisible()
        .setPageId("pageid1").setClientTime(1400000000100l).build()));

    // Page 2
    values.add(new AvroValue<TrackerEvent>(MakeBasicImageLoad()
        .setPageId("pageid2").setRefURL("http://go.com")
        .setPageURL("http://there.com").setClientTime(1400000010000l).build()));

    // Page 3 (back to the first page)
    values.add(new AvroValue<TrackerEvent>(MakeBasicImageLoad()
        .setPageId("pageid3").setRefURL("http://there.com")
        .setPageURL("http://go.com").setClientTime(1400000020000l).build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicImageVisible()
        .setPageId("pageid3").setRefURL("http://there.com")
        .setPageURL("http://go.com").setClientTime(1400000020100l).build()));

    reduceDriver.withInput(new Text("tai156.45.41.124vid1"), values).run();
    CaptureEvents();

    // There should be three sequences, one for each page visited
    VerifySequence(Arrays.asList(MakeBasicImageLoadHive().setPageId("pageid1")
        .build(), MakeBasicImageVisibleHive().setPageId("pageid1")
        .setClientTime(1400000000.1).build(), MakeBasicEventSequenceHive()
        .setImVisClientTime(1400000000.1).setImLoadClientTime(1400000000.)
        .setImLoadPageURL("http://go.com").setImLoadServerTime(1400697546.)
        .setImVisServerTime(1400697546.).setServerTime(1400697546.)
        .setThumbnailId("acct1_vid1_tid1").setImLoadPageId("pageid1")
        .setImVisPageId("pageid1").setWidth(640).setHeight(480).build()));

    VerifySequence(Arrays.asList(
        MakeBasicImageLoadHive().setClientTime(1400000010.)
            .setPageId("pageid2").setRefURL("http://go.com")
            .setPageURL("http://there.com").build(),
        MakeBasicEventSequenceHive().setImLoadClientTime(1400000010.)
            .setImLoadPageURL("http://there.com")
            .setImLoadServerTime(1400697546.).setRefURL("http://go.com")
            .setServerTime(1400697546.).setThumbnailId("acct1_vid1_tid1")
            .setImLoadPageId("pageid2").setWidth(640).setHeight(480)
            .setAgentInfoBrowserName("Internet Explorer")
            .setAgentInfoBrowserVersion("10.01").setAgentInfoOsName("Windows")
            .setAgentInfoOsVersion("7").build()));

    VerifySequence(Arrays.asList(
        MakeBasicImageLoadHive().setClientTime(1400000020.)
            .setPageId("pageid3").setRefURL("http://there.com").build(),
        MakeBasicImageVisibleHive().setPageId("pageid3")
            .setClientTime(1400000020.1).setRefURL("http://there.com").build(),
        MakeBasicEventSequenceHive().setImVisClientTime(1400000020.1)
            .setImLoadClientTime(1400000020.).setImLoadPageURL("http://go.com")
            .setRefURL("http://there.com").setImLoadServerTime(1400697546.)
            .setImVisServerTime(1400697546.).setServerTime(1400697546.)
            .setThumbnailId("acct1_vid1_tid1").setImLoadPageId("pageid3")
            .setImVisPageId("pageid3").setWidth(640).setHeight(480).build()));

  }

  @Test
  public void testImageCoordsNull() throws IOException, InterruptedException {
    List<AvroValue<TrackerEvent>> values =
        new ArrayList<AvroValue<TrackerEvent>>();
    values.add(new AvroValue<TrackerEvent>(MakeBasicImageClick().setEventData(
        new ImageClick(true, "acct1_vid1_tid1", new Coords(100f, 200f),
            new Coords(150f, 250f), null)).build()));

    reduceDriver.withInput(new Text("tai156.45.41.124vid1"), values).run();
    CaptureEvents();

    VerifySequence(Arrays.asList(
        MakeBasicImageClickHive().setImageCoordsX(null).setImageCoordsY(null)
            .build(),
        MakeBasicEventSequenceHive().setImClickPageURL("http://go.com")
            .setImClickClientTime(1400000000.)
            .setImClickServerTime(1400697546.).setServerTime(1400697546.)
            .setThumbnailId("acct1_vid1_tid1").setPageCoordsX(100f)
            .setPageCoordsY(200f).setWindowCoordsX(150f).setWindowCoordsY(250f)
            .setImageCoordsX(null).setImageCoordsY(null)
            .setIsClickInPlayer(false).setIsRightClick(false)
            .setImClickPageId("pageid1").build()));
  }

  @Test
  public void testPPGVideoClick() throws IOException, InterruptedException {
    TrackerEvent inputEvent =
        TrackerEvent
            .newBuilder()
            .setEventType(EventType.VIDEO_CLICK)
            .setIpGeoData(new GeoData(null, null, null, null, null, null))
            .setPageId("lgFQCAy0PT5szYSr")
            .setNeonUserId("")
            .setTrackerAccountId("1483115066")
            .setServerTime(1412111219989l)
            .setClientTime(1412109680684l)
            .setPageURL("http://www.post-gazette.com/")
            .setRefURL("https://www.google.com/")
            .setTrackerType(TrackerType.BRIGHTCOVE)
            .setUserAgent(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36")
            .setClientIP("199.47.77.98")
            .setAgentInfo(
                AgentInfo.newBuilder()
                    .setBrowser(new NmVers("Chrome", "35.0.1916.114"))
                    .setOs(new NmVers("Linux", null)).build())
            .setEventData(
                new VideoClick(
                    true,
                    "3811926729001",
                    "1395884386001",
                    "6d3d519b15600c372a1f6735711d956e_3811926729001_7652d5c345f87dbfe517527c21045cc8"))
            .build();
    List<AvroValue<TrackerEvent>> values =
        new ArrayList<AvroValue<TrackerEvent>>();
    values.add(new AvroValue<TrackerEvent>(inputEvent));

    reduceDriver.withInput(new Text("1483115066199.47.77.983811926729001"),
        values).run();
    CaptureEvents();

    VerifySequence(Arrays
        .asList(
            ImageClickHive
                .newBuilder()
                .setSequenceId(1)
                .setIpGeoDataCity(null)
                .setIpGeoDataCountry(null)
                .setIpGeoDataRegion(null)
                .setIpGeoDataZip(null)
                .setIpGeoDataLat(null)
                .setIpGeoDataLon(null)
                .setImageCoordsX(-1.0f)
                .setImageCoordsY(-1.0f)
                .setPageCoordsX(-1.0f)
                .setPageCoordsY(-1.0f)
                .setWindowCoordsX(-1.0f)
                .setWindowCoordsY(-1.0f)
                .setPageId("lgFQCAy0PT5szYSr")
                .setNeonUserId("")
                .setTrackerAccountId("1483115066")
                .setServerTime(1412111219.989)
                .setClientTime(1412109680.684)
                .setPageURL("http://www.post-gazette.com/")
                .setRefURL("https://www.google.com/")
                .setTrackerType(TrackerType.BRIGHTCOVE)
                .setUserAgent(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36")
                .setClientIP("199.47.77.98")
                .setAgentInfoBrowserName("Chrome")
                .setAgentInfoBrowserVersion("35.0.1916.114")
                .setAgentInfoOsName("Linux")
                .setAgentInfoOsVersion(null)
                .setIsClickInPlayer(true)
                .setThumbnailId(
                    "6d3d519b15600c372a1f6735711d956e_3811926729001_7652d5c345f87dbfe517527c21045cc8")
                .setVideoId("3811926729001").setIsRightClick(false).build(),
            EventSequenceHive
                .newBuilder()
                .setSequenceId(1)
                .setIpGeoDataCity(null)
                .setIpGeoDataCountry(null)
                .setIpGeoDataRegion(null)
                .setIpGeoDataZip(null)
                .setIpGeoDataLat(null)
                .setIpGeoDataLon(null)
                .setPlayerId("1395884386001")
                .setImClickPageId("lgFQCAy0PT5szYSr")
                .setNeonUserId("")
                .setTrackerAccountId("1483115066")
                .setServerTime(1412111219.989)
                .setImClickServerTime(1412111219.989)
                .setImClickClientTime(1412109680.684)
                .setImClickPageURL("http://www.post-gazette.com/")
                .setRefURL("https://www.google.com/")
                .setTrackerType(TrackerType.BRIGHTCOVE)
                .setUserAgent(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36")
                .setClientIP("199.47.77.98")
                .setAgentInfoBrowserName("Chrome")
                .setAgentInfoBrowserVersion("35.0.1916.114")
                .setAgentInfoOsName("Linux")
                .setAgentInfoOsVersion(null)
                .setIsClickInPlayer(true)
                .setThumbnailId(
                    "6d3d519b15600c372a1f6735711d956e_3811926729001_7652d5c345f87dbfe517527c21045cc8")
                .setVideoId("3811926729001").setIsRightClick(false).build()));
  }

  @Test
  public void testSequenceCollision() throws IOException, InterruptedException {
    // We can get sequence id collisions if they the lower bits are not merged
    // properly. This test checks that case.
    List<AvroValue<TrackerEvent>> values =
        new ArrayList<AvroValue<TrackerEvent>>();
    // Page 1
    values
        .add(new AvroValue<TrackerEvent>(
            MakeBasicImageLoad()
                .setPageId("SR5CxDD7O7F1qJr0")
                .setTrackerAccountId("1483115066")
                .setClientIP("24.3.189.49")
                .setClientTime(1403640571040l)
                .setServerTime(1403640570481l)
                .setEventData(
                    new ImageLoad(
                        "6d3d519b15600c372a1f6735711d956e_3627714179001_5e48f414cd3a62ce9cfa6d68762e6e50",
                        90, 120)).build()));

    // Page 2
    values
        .add(new AvroValue<TrackerEvent>(
            MakeBasicImageLoad()
                .setPageId("j3bFwKuSFyhekQrr")
                .setTrackerAccountId("1483115066")
                .setClientIP("24.3.189.49")
                .setClientTime(1403036490566l)
                .setServerTime(1403036487794l)
                .setEventData(
                    new ImageLoad(
                        "6d3d519b15600c372a1f6735711d956e_3627714179001_5e48f414cd3a62ce9cfa6d68762e6e50",
                        0, 200)).build()));

    reduceDriver.withInput(new Text("tai156.45.41.124vid1"), values).run();
    CaptureEvents();

    VerifySequence(Arrays
        .asList(
            MakeBasicImageLoadHive()
                .setPageId("SR5CxDD7O7F1qJr0")
                .setTrackerAccountId("1483115066")
                .setClientIP("24.3.189.49")
                .setClientTime(1403640571.040)
                .setServerTime(1403640570.481)
                .setHeight(90)
                .setWidth(120)
                .setThumbnailId(
                    "6d3d519b15600c372a1f6735711d956e_3627714179001_5e48f414cd3a62ce9cfa6d68762e6e50")
                .build(),
            MakeBasicEventSequenceHive()
                .setImLoadClientTime(1403640571.040)
                .setImLoadServerTime(1403640570.481)
                .setServerTime(1403640570.481)
                .setClientIP("24.3.189.49")
                .setTrackerAccountId("1483115066")
                .setThumbnailId(
                    "6d3d519b15600c372a1f6735711d956e_3627714179001_5e48f414cd3a62ce9cfa6d68762e6e50")
                .setImLoadPageId("SR5CxDD7O7F1qJr0")
                .setImLoadPageURL("http://go.com").setWidth(120).setHeight(90)
                .setAgentInfoBrowserName("Internet Explorer")
                .setAgentInfoBrowserVersion("10.01")
                .setAgentInfoOsName("Windows").setAgentInfoOsVersion("7")
                .build()));

    VerifySequence(Arrays
        .asList(
            MakeBasicImageLoadHive()
                .setPageId("j3bFwKuSFyhekQrr")
                .setTrackerAccountId("1483115066")
                .setClientIP("24.3.189.49")
                .setClientTime(1403036490.566)
                .setServerTime(1403036487.794)
                .setHeight(0)
                .setWidth(200)
                .setThumbnailId(
                    "6d3d519b15600c372a1f6735711d956e_3627714179001_5e48f414cd3a62ce9cfa6d68762e6e50")
                .build(),
            MakeBasicEventSequenceHive()
                .setImLoadClientTime(1403036490.566)
                .setImLoadServerTime(1403036487.794)
                .setServerTime(1403036487.794)
                .setClientIP("24.3.189.49")
                .setTrackerAccountId("1483115066")
                .setThumbnailId(
                    "6d3d519b15600c372a1f6735711d956e_3627714179001_5e48f414cd3a62ce9cfa6d68762e6e50")
                .setImLoadPageId("j3bFwKuSFyhekQrr")
                .setImLoadPageURL("http://go.com").setWidth(200).setHeight(0)
                .setAgentInfoBrowserName("Internet Explorer")
                .setAgentInfoBrowserVersion("10.01")
                .setAgentInfoOsName("Windows").setAgentInfoOsVersion("7")
                .build()));
  }
}
