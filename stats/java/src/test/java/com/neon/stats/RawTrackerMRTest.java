package com.neon.stats;

import java.io.IOException;
import java.util.*;

import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.neon.stats.RawTrackerMR.*;
import com.neon.Tracker.*;

public class RawTrackerMRTest {
  MapDriver<AvroKey<TrackerEvent>, NullWritable, Text, AvroValue<TrackerEvent>> mapDriver;
  ReduceDriver<Text, AvroValue<TrackerEvent>, WritableComparable<Object>, Writable> reduceDriver;
  MapReduceDriver<AvroKey<TrackerEvent>, NullWritable, Text, AvroValue<TrackerEvent>, WritableComparable<Object>, Writable> mapReduceDriver;

  @Before
  public void setUp() throws IOException {
    MapToUserEventStream mapper = new MapToUserEventStream();
    CleanUserStreamReducer reducer = new CleanUserStreamReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    Configuration conf = mapDriver.getConfiguration();
    AvroSerialization.addToConfiguration(conf);
    AvroSerialization.setKeyWriterSchema(conf, TrackerEvent.getClassSchema());
    AvroSerialization.setKeyReaderSchema(conf, TrackerEvent.getClassSchema());
    AvroSerialization.setValueWriterSchema(conf, TrackerEvent.getClassSchema());
    AvroSerialization.setValueReaderSchema(conf, TrackerEvent.getClassSchema());
    Job job = new Job(conf);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(AvroValue.class);
    AvroJob.setInputKeySchema(job, TrackerEvent.getClassSchema());
    AvroJob.setMapOutputValueSchema(job, TrackerEvent.getClassSchema());

    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test
  public void testImagesVisibleMapping() throws IOException {
    TrackerEvent baseEvent =
        new TrackerEvent("pageid1", "tai1", TrackerType.BRIGHTCOVE,
            "http://go.com", "http://ref.com", 6498474l, 6549136l,
            "56.45.41.124", "", "agent1", null, new GeoData("USA", null, "CA",
                null, 34.556f, 120.45f), EventType.IMAGES_VISIBLE, null);

    ImagesVisible inputEventData =
        new ImagesVisible(true, Arrays.asList((CharSequence) "acct1_vid1_tid1",
            "acct1_vid2_tid12", "acct_vid31_tid0"));
    mapDriver.withInput(
        new AvroKey<TrackerEvent>(TrackerEvent.newBuilder(baseEvent)
            .setEventData(inputEventData).build()), NullWritable.get());

    mapDriver.withOutput(
        new Text("tai156.45.41.124vid1"),
        new AvroValue<TrackerEvent>(TrackerEvent.newBuilder(baseEvent)
            .setEventType(EventType.IMAGE_VISIBLE)
            .setEventData(new ImageVisible("acct1_vid1_tid1")).build()));
    mapDriver.withOutput(
        new Text("tai156.45.41.124vid2"),
        new AvroValue<TrackerEvent>(TrackerEvent.newBuilder(baseEvent)
            .setEventType(EventType.IMAGE_VISIBLE)
            .setEventData(new ImageVisible("acct1_vid2_tid12")).build()));
    mapDriver.withOutput(
        new Text("tai156.45.41.124vid31"),
        new AvroValue<TrackerEvent>(TrackerEvent.newBuilder(baseEvent)
            .setEventType(EventType.IMAGE_VISIBLE)
            .setEventData(new ImageVisible("acct_vid31_tid0")).build()));
    mapDriver.runTest();
  }

  @Test
  public void testImagesLoadedMapping() throws IOException {
    TrackerEvent baseEvent =
        new TrackerEvent("pageid1", "tai1", TrackerType.BRIGHTCOVE,
            "http://go.com", "http://ref.com", 6498474l, 6549136l,
            "56.45.41.124", "", "agent1", null, new GeoData("USA", "San Fran",
                "CA", "94132", 34.556f, 120.45f), EventType.IMAGES_LOADED, null);

    List<ImageLoad> loadList = new Vector<ImageLoad>();
    loadList.add(new ImageLoad("acct1_vid1_tid1", 56, 48));
    loadList.add(new ImageLoad("acct1_vid3_tid46", 480, 640));

    mapDriver.withInput(
        new AvroKey<TrackerEvent>(TrackerEvent.newBuilder(baseEvent)
            .setEventData(new ImagesLoaded(true, loadList)).build()),
        NullWritable.get());

    mapDriver.withOutput(
        new Text("tai156.45.41.124vid1"),
        new AvroValue<TrackerEvent>(TrackerEvent.newBuilder(baseEvent)
            .setEventType(EventType.IMAGE_LOAD)
            .setEventData(new ImageLoad("acct1_vid1_tid1", 56, 48)).build()));
    mapDriver
        .withOutput(
            new Text("tai156.45.41.124vid3"),
            new AvroValue<TrackerEvent>(TrackerEvent.newBuilder(baseEvent)
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
        new TrackerEvent("pageid1", "tai1", TrackerType.BRIGHTCOVE,
            "http://go.com", "http://ref.com", 6498474l, 6549136l,
            "56.45.41.124", "", "agent1", null, new GeoData("USA", "San Fran",
                "CA", "94132", 34.556f, 120.45f), EventType.VIDEO_CLICK,
            new VideoClick(true, "vid2", "player2", null));

    mapDriver.withInput(new AvroKey<TrackerEvent>(inputEvent),
        NullWritable.get());

    mapDriver.withOutput(new Text("tai156.45.41.124vid2"),
        new AvroValue<TrackerEvent>(inputEvent));
    
    mapDriver.runTest();
  }
  
  @Test
  public void testVideoPlayMapping() throws IOException {
    TrackerEvent inputEvent =
        new TrackerEvent("pageid1", "tai1", TrackerType.BRIGHTCOVE,
            "http://go.com", "http://ref.com", 6498474l, 6549136l,
            "56.45.41.124", "", "agent1", null, new GeoData("USA", "San Fran",
                "CA", "94132", 34.556f, 120.45f), EventType.VIDEO_PLAY,
            new VideoPlay(true, "vid2", "player2", null, true, 600, 1));

    mapDriver.withInput(new AvroKey<TrackerEvent>(inputEvent),
        NullWritable.get());

    mapDriver.withOutput(new Text("tai156.45.41.124vid2"),
        new AvroValue<TrackerEvent>(inputEvent));
    
    mapDriver.runTest();
  }
  
  @Test
  public void testAdPlayMapping() throws IOException {
    TrackerEvent inputEvent =
        new TrackerEvent("pageid1", "tai1", TrackerType.BRIGHTCOVE,
            "http://go.com", "http://ref.com", 6498474l, 6549136l,
            "56.45.41.124", "", "agent1", null, new GeoData("USA", "San Fran",
                "CA", "94132", 34.556f, 120.45f), EventType.AD_PLAY,
            new AdPlay(true, "vid2", "player2", null, null, 2));

    mapDriver.withInput(new AvroKey<TrackerEvent>(inputEvent),
        NullWritable.get());

    mapDriver.withOutput(new Text("tai156.45.41.124vid2"),
        new AvroValue<TrackerEvent>(inputEvent));
    
    mapDriver.runTest();
  }
}
