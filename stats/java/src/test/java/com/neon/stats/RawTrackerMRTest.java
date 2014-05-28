package com.neon.stats;

import java.io.IOException;
import java.util.*;

import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.avro.specific.SpecificRecordBase;
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
    return ImageLoadHive
        .newBuilder()
        .setAgentInfo(null)
        .setClientIP("56.45.41.124")
        .setClientTime(1400000000f)
        .setIpGeoData(new GeoData("USA", null, "CA", null, 34.556f, 120.45f))
        .setNeonUserId("")
        .setPageId("pageid1")
        .setPageURL("http://go.com")
        .setRefURL("http://ref.com")
        .setServerTime(1400697546f)
        .setTrackerType(TrackerType.BRIGHTCOVE)
        .setTrackerAccountId("tai1")
        .setUserAgent("agent1")
        .setHeight(480)
        .setWidth(640)
        .setThumbnailId("acct1_vid1_tid1")
        .setAgentInfo(
            new AgentInfo(new NmVers("Windows", "7"), new NmVers(
                "Internet Explorer", "10.01")));
  }

  protected TrackerEvent.Builder MakeBasicImageVisible() {
    return MakeBasicTrackerEvent().setEventType(EventType.IMAGE_VISIBLE)
        .setEventData(new ImageVisible("acct1_vid1_tid1"));
  }

  protected ImageVisibleHive.Builder MakeBasicImageVisibleHive() {
    return ImageVisibleHive.newBuilder().setAgentInfo(null)
        .setClientIP("56.45.41.124").setClientTime(1400000000f)
        .setIpGeoData(new GeoData("USA", null, "CA", null, 34.556f, 120.45f))
        .setNeonUserId("").setPageId("pageid1").setPageURL("http://go.com")
        .setRefURL("http://ref.com").setTrackerAccountId("tai1")
        .setServerTime(1400697546f).setTrackerType(TrackerType.BRIGHTCOVE)
        .setUserAgent("agent1").setThumbnailId("acct1_vid1_tid1");
  }

  protected TrackerEvent.Builder MakeBasicImageClick() {
    return MakeBasicTrackerEvent().setEventType(EventType.IMAGE_CLICK)
        .setEventData(
            new ImageClick(true, "acct1_vid1_tid1", new Coords(100f, 200f),
                new Coords(150f, 250f)));
  }

  protected ImageClickHive.Builder MakeBasicImageClickHive() {
    return ImageClickHive.newBuilder().setAgentInfo(null)
        .setClientIP("56.45.41.124").setClientTime(1400000000f)
        .setIpGeoData(new GeoData("USA", null, "CA", null, 34.556f, 120.45f))
        .setNeonUserId("").setPageId("pageid1").setPageURL("http://go.com")
        .setRefURL("http://ref.com").setTrackerAccountId("tai1")
        .setServerTime(1400697546f).setTrackerType(TrackerType.BRIGHTCOVE)
        .setUserAgent("agent1").setThumbnailId("acct1_vid1_tid1")
        .setVideoId("vid1").setPageCoords(new Coords(100f, 200f))
        .setWindowCoords(new Coords(150f, 250f)).setIsClickInPlayer(false)
        .setIsRightClick(false);
  }

  protected TrackerEvent.Builder MakeBasicAdPlay() {
    return MakeBasicTrackerEvent().setEventType(EventType.AD_PLAY)
        .setEventData(
            new AdPlay(true, "vid1", "player2", "acct1_vid1_tid1", null, 1));
  }

  protected AdPlayHive.Builder MakeBasicAdPlayHive() {
    return AdPlayHive.newBuilder().setAgentInfo(null)
        .setClientIP("56.45.41.124").setClientTime(1400000000f)
        .setIpGeoData(new GeoData("USA", null, "CA", null, 34.556f, 120.45f))
        .setNeonUserId("").setPageId("pageid1").setPageURL("http://go.com")
        .setRefURL("http://ref.com").setTrackerAccountId("tai1")
        .setServerTime(1400697546f).setTrackerType(TrackerType.BRIGHTCOVE)
        .setUserAgent("agent1").setThumbnailId("acct1_vid1_tid1")
        .setVideoId("vid1").setPlayerId("player2").setAutoplayDelta(null)
        .setPlayCount(1);
  }

  protected TrackerEvent.Builder MakeBasicVideoClick() {
    return MakeBasicTrackerEvent().setEventType(EventType.VIDEO_CLICK)
        .setEventData(
            new VideoClick(true, "vid1", "player2", "acct1_vid1_tid1"));
  }

  protected TrackerEvent.Builder MakeBasicVideoPlay() {
    return MakeBasicTrackerEvent().setEventType(EventType.VIDEO_PLAY)
        .setEventData(
            new VideoPlay(true, "vid1", "player2", "acct1_vid1_tid1", true,
                null, 1));
  }

  protected VideoPlayHive.Builder MakeBasicVideoPlayHive() {
    return VideoPlayHive.newBuilder().setAgentInfo(null)
        .setClientIP("56.45.41.124").setClientTime(1400000000f)
        .setIpGeoData(new GeoData("USA", null, "CA", null, 34.556f, 120.45f))
        .setNeonUserId("").setPageId("pageid1").setPageURL("http://go.com")
        .setRefURL("http://ref.com").setTrackerAccountId("tai1")
        .setServerTime(1400697546f).setTrackerType(TrackerType.BRIGHTCOVE)
        .setUserAgent("agent1").setThumbnailId("acct1_vid1_tid1")
        .setVideoId("vid1").setPlayerId("player2").setAutoplayDelta(null)
        .setPlayCount(1).setDidAdPlay(true);
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
            .setEventData(new ImageVisible("acct-vid31-tid0")).build()));
    mapDriver.runTest();
  }

  @Test
  public void testImagesLoadedMapping() throws IOException {
    TrackerEvent.Builder baseEvent = MakeBasicTrackerEvent();

    List<ImageLoad> loadList = new Vector<ImageLoad>();
    loadList.add(new ImageLoad("acct1_vid1_tid1", 56, 48));
    loadList.add(new ImageLoad("acct1_vid3_tid46", 480, 640));

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
                new Coords(78f, 69f)));

    mapDriver.withInput(new AvroKey<TrackerEvent>(inputEvent),
        NullWritable.get());

    mapDriver.withOutput(new Text("tai156.45.41.124vid2"),
        new AvroValue<TrackerEvent>(inputEvent));

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
  public void testVideoPlayMapping() throws IOException {
    TrackerEvent inputEvent =
        MakeBasicTrackerEvent()
            .setEventType(EventType.VIDEO_PLAY)
            .setEventData(
                new VideoPlay(true, "vid2", "player2", null, true, 600, 1))
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
        MakeBasicTrackerEvent().setEventType(EventType.AD_PLAY)
            .setEventData(new AdPlay(true, "vid2", "player2", null, null, 2))
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
    values.add(new AvroValue<TrackerEvent>(MakeBasicAdPlay()
        .setClientTime(1400000000500l).setPageId("vidpage")
        .setRefURL("http://go.com").build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicVideoClick()
        .setClientTime(1400000000600l).setPageId("vidpage")
        .setRefURL("http://go.com").build()));
    values.add(new AvroValue<TrackerEvent>(MakeBasicVideoPlay()
        .setClientTime(1400000000700l).setPageId("vidpage")
        .setRefURL("http://go.com").build()));

    // We have to look at the outputs in the mock because MRUnit doesn't handle
    // MultipleOutputs
    reduceDriver.withInput(new Text("tai156.45.41.124vid1"), values).run();

    ArgumentCaptor<AvroKey> hiveEventCaptor =
        ArgumentCaptor.forClass(AvroKey.class);
    ArgumentCaptor<String> outputPathCaptor =
        ArgumentCaptor.forClass(String.class);
    verify(outputCollector).write(eq("ImageLoadHive"),
        hiveEventCaptor.capture(), eq(NullWritable.get()),
        outputPathCaptor.capture());
    verify(outputCollector).write(eq("ImageVisibleHive"),
        hiveEventCaptor.capture(), eq(NullWritable.get()),
        outputPathCaptor.capture());
    verify(outputCollector).write(eq("ImageClickHive"),
        hiveEventCaptor.capture(), eq(NullWritable.get()),
        outputPathCaptor.capture());
    verify(outputCollector).write(eq("VideoPlayHive"),
        hiveEventCaptor.capture(), eq(NullWritable.get()),
        outputPathCaptor.capture());
    verify(outputCollector).write(eq("AdPlayHive"), hiveEventCaptor.capture(),
        eq(NullWritable.get()), outputPathCaptor.capture());

    List<AvroKey> hiveEvents = hiveEventCaptor.getAllValues();
    List<String> outputPaths = outputPathCaptor.getAllValues();
    assertEquals(hiveEvents.size(), outputPaths.size());
    assertEquals(hiveEvents.size(), 5);
    Long sequenceId = null;
    for (int i = 0; i < hiveEvents.size(); ++i) {
      AvroKey curEvent = hiveEvents.get(i);
      if (curEvent.datum() instanceof ImageLoadHive) {
        sequenceId =
            sequenceId == null ? ((ImageLoadHive) curEvent.datum()).getSequenceId()
                : sequenceId;
        assertEquals(curEvent.datum(),
            MakeBasicImageLoadHive().setSequenceId(sequenceId).build());
        assertEquals(outputPaths.get(i), "ImageLoadHive/tai=tai1/ts=2014-05-21/ImageLoadHive");
      } else if (curEvent.datum() instanceof ImageVisibleHive) {
        sequenceId =
            sequenceId == null ? ((ImageVisibleHive) curEvent.datum()).getSequenceId()
                : sequenceId;
        assertEquals(curEvent.datum(),
            MakeBasicImageVisibleHive().setClientTime(1400000000.1f)
                .setSequenceId(sequenceId).build());
        assertEquals(outputPaths.get(i),
            "ImageVisibleHive/tai=tai1/ts=2014-05-21/ImageVisibleHive");
      } else if (curEvent.datum() instanceof ImageClickHive) {
        sequenceId =
            sequenceId == null ? ((ImageClickHive) curEvent.datum()).getSequenceId()
                : sequenceId;
        assertEquals(curEvent.datum(),
            MakeBasicImageClickHive().setClientTime(1400000000.2f)
                .setSequenceId(sequenceId).build());
        assertEquals(outputPaths.get(i),
            "ImageClickHive/tai=tai1/ts=2014-05-21/ImageClickHive");
      } else if (curEvent.datum() instanceof AdPlayHive) {
        sequenceId =
            sequenceId == null ? ((AdPlayHive) curEvent.datum()).getSequenceId()
                : sequenceId;
        assertEquals(
            curEvent.datum(),
            MakeBasicAdPlayHive().setClientTime(1400000000.5f)
                .setPageId("vidpage").setRefURL("http://go.com")
                .setSequenceId(sequenceId).build());
        assertEquals(outputPaths.get(i), "AdPlayHive/tai=tai1/ts=2014-05-21/AdPlayHive");
      } else if (curEvent.datum() instanceof VideoPlayHive) {
        sequenceId =
            sequenceId == null ? ((VideoPlayHive) curEvent.datum()).getSequenceId()
                : sequenceId;
        assertEquals(
            curEvent.datum(),
            MakeBasicVideoPlayHive().setClientTime(1400000000.7f)
                .setPageId("vidpage").setSequenceId(sequenceId)
                .setRefURL("http://go.com").build());
        assertEquals(outputPaths.get(i), "VideoPlayHive/tai=tai1/ts=2014-05-21/VideoPlayHive");
      }
    }

  }
}
