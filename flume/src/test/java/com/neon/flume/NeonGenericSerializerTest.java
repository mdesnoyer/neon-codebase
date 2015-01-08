package com.neon.flume;

import com.neon.Tracker.*;
import org.junit.*;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.avro.Schema;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.hbase.async.AtomicIncrementRequest;

import org.apache.avro.util.Utf8;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import org.apache.flume.event.EventBuilder;

import org.json.*;

import static org.mockito.Mockito.*;

public class NeonGenericSerializerTest {
  private static String VALID_SCHEMA_URL = "https://some_stored_schema.avsc";

  private NeonGenericSerializer.URLOpener mockURL;
  private NeonGenericSerializer serializer;
  private static TestAppender testAppender;
  private static Logger logger;

  @Before
  public void setUp() throws IOException {
    // Add the logger
    testAppender = new TestAppender();
    logger = Logger.getLogger(NeonGenericSerializer.class.getName());
    logger.addAppender(testAppender);

    // Mock out the http call to get the schema
    mockURL = mock(NeonGenericSerializer.URLOpener.class);

    // Create the serializer
    serializer = new NeonGenericSerializer(mockURL);
    serializer.configure(new Context());
  }

  @After
  public void tearDown() {
    // Remove the logger
    logger.removeAppender(testAppender);
  }

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testImageVisibleBase() throws Exception {
    // Build the event
    ImageVisible visEvent =
        ImageVisible.newBuilder().setThumbnailId("acct1_vid1_thumb1").build();
    TrackerEvent event =
        buildDefaultEvent().setEventType(EventType.IMAGE_VISIBLE)
            .setEventData(visEvent).build();
    Event avroEvent = buildAvroEvent(event);

    // Test
    serializer.setEvent(avroEvent);

    // Check the puts
    assertEquals(serializer.getActions().size(), 0);

    // Check the increments
    assertEquals(serializer.getIncrements().size(), 2);
    assertIncrementFound("THUMBNAIL_TIMESTAMP_EVENT_COUNTS",
        "acct1_vid1_thumb1_2014-11-21T23", "evts", "iv", 1);
    assertIncrementFound("TIMESTAMP_THUMBNAIL_EVENT_COUNTS",
        "2014-11-21T23_acct1_vid1_thumb1", "evts", "iv", 1);
  }

  @Test
  public void testEventSequence() throws Exception {
    // Build the load levent
    ImageLoad loadEvent =
        ImageLoad.newBuilder().setThumbnailId("acct1_vid1_thumb1")
            .setHeight(600).setWidth(400).build();
    TrackerEvent event =
        buildDefaultEvent().setEventType(EventType.IMAGE_LOAD)
            .setEventData(loadEvent).build();
    Event avroEvent = buildAvroEvent(event);

    // Test
    serializer.setEvent(avroEvent);
    assertEquals(serializer.getActions().size(), 0);
    assertEquals(serializer.getIncrements().size(), 2);
    assertIncrementFound("THUMBNAIL_TIMESTAMP_EVENT_COUNTS",
        "acct1_vid1_thumb1_2014-11-21T23", "evts", "il", 1);
    assertIncrementFound("TIMESTAMP_THUMBNAIL_EVENT_COUNTS",
        "2014-11-21T23_acct1_vid1_thumb1", "evts", "il", 1);

    // Build the visible event
    ImageVisible visEvent =
        ImageVisible.newBuilder().setThumbnailId("acct1_vid1_thumb1").build();
    event =
        buildDefaultEvent().setEventType(EventType.IMAGE_VISIBLE)
            .setEventData(visEvent).build();
    avroEvent = buildAvroEvent(event);

    // Test
    serializer.setEvent(avroEvent);
    assertEquals(serializer.getActions().size(), 0);
    assertEquals(serializer.getIncrements().size(), 2);
    assertIncrementFound("THUMBNAIL_TIMESTAMP_EVENT_COUNTS",
        "acct1_vid1_thumb1_2014-11-21T23", "evts", "iv", 1);
    assertIncrementFound("TIMESTAMP_THUMBNAIL_EVENT_COUNTS",
        "2014-11-21T23_acct1_vid1_thumb1", "evts", "iv", 1);
  }
  
  @Test
  public void testGoodEventThenFailedEvent() throws IOException {
    // Build a good visible event
    ImageVisible visEvent =
        ImageVisible.newBuilder().setThumbnailId("acct1_vid1_thumb1").build();
    TrackerEvent event =
        buildDefaultEvent().setEventType(EventType.IMAGE_VISIBLE)
            .setEventData(visEvent).build();
    Event avroEvent = buildAvroEvent(event);

    // Test the good event
    serializer.setEvent(avroEvent);
    assertEquals(serializer.getActions().size(), 0);
    assertEquals(serializer.getIncrements().size(), 2);
    assertIncrementFound("THUMBNAIL_TIMESTAMP_EVENT_COUNTS",
        "acct1_vid1_thumb1_2014-11-21T23", "evts", "iv", 1);
    assertIncrementFound("TIMESTAMP_THUMBNAIL_EVENT_COUNTS",
        "2014-11-21T23_acct1_vid1_thumb1", "evts", "iv", 1);
    
    // Now run an invalid event because the schema that's referred to is incorrect

    // Mock out the schema at the url so that it's the wrong schema
    String badSchema =
        "{\"type\":\"record\", \"name\":\"TrackerEvent\", \"fields\":[{\"name\":\"pageUrl\", \"type\":\"string\"}]}";
    when(mockURL.open("http://wrong_schema.avsc")).thenReturn(
        new ByteArrayInputStream(badSchema.getBytes()));
    avroEvent.getHeaders().put("flume.avro.schema.url", "http://wrong_schema.avsc");
    
    serializer.setEvent(avroEvent);

    // Check the outputs
    assertEquals(serializer.getActions().size(), 0);
    assertEquals(serializer.getIncrements().size(), 0);
    
    assertLogExists(Level.ERROR, "Error parsing avro event");
    
  }

  @Test
  public void testImageNewField() throws Exception {
    // Create the schema with a new field in it
    JSONObject schemaJson =
        new JSONObject(TrackerEvent.getClassSchema().toString());
    schemaJson
        .getJSONArray("fields")
        .put(
            new JSONObject(
                "{\"name\": \"dummyField\", \"type\" : [ \"null\" , \"int\" ], \"default\" : \"null\"}"));
    Schema changedSchema = new Schema.Parser().parse(schemaJson.toString());

    // Build the event
    ImageVisible visEvent =
        ImageVisible.newBuilder().setThumbnailId("acct1_vid1_thumb1").build();
    GenericRecord event =
        buildDefaultGenericEvent(changedSchema).set("dummyField", 78)
            .set("eventType", EventType.IMAGE_VISIBLE)
            .set("eventData", visEvent).build();
    Event avroEvent =
        buildAvroEvent(event, changedSchema, "http://extra_field.avsc");

    serializer.setEvent(avroEvent);

    // Check the puts
    assertEquals(serializer.getActions().size(), 0);

    // Check the increments
    assertEquals(serializer.getIncrements().size(), 2);
    assertIncrementFound("THUMBNAIL_TIMESTAMP_EVENT_COUNTS",
        "acct1_vid1_thumb1_2014-11-21T23", "evts", "iv", 1);
    assertIncrementFound("TIMESTAMP_THUMBNAIL_EVENT_COUNTS",
        "2014-11-21T23_acct1_vid1_thumb1", "evts", "iv", 1);
  }

  @Test
  public void testImagesVisible() throws Exception {
    // Build the event
    ImagesVisible visEvent =
        new ImagesVisible(true, Arrays.asList(
            (CharSequence) "acct1_vid1_thumb1", "acct1_vid2_thumb2"));
    TrackerEvent event =
        buildDefaultEvent().setEventType(EventType.IMAGES_VISIBLE)
            .setEventData(visEvent).build();
    Event avroEvent = buildAvroEvent(event);

    // Test
    serializer.setEvent(avroEvent);

    // Check the puts
    assertEquals(serializer.getActions().size(), 0);

    // Check the increments
    assertEquals(serializer.getIncrements().size(), 4);
    assertIncrementFound("THUMBNAIL_TIMESTAMP_EVENT_COUNTS",
        "acct1_vid1_thumb1_2014-11-21T23", "evts", "iv", 1);
    assertIncrementFound("THUMBNAIL_TIMESTAMP_EVENT_COUNTS",
        "acct1_vid2_thumb2_2014-11-21T23", "evts", "iv", 1);
    assertIncrementFound("TIMESTAMP_THUMBNAIL_EVENT_COUNTS",
        "2014-11-21T23_acct1_vid1_thumb1", "evts", "iv", 1);
    assertIncrementFound("TIMESTAMP_THUMBNAIL_EVENT_COUNTS",
        "2014-11-21T23_acct1_vid2_thumb2", "evts", "iv", 1);
  }

  @Test
  public void testImageClick() throws Exception {
    // Build the event
    ImageClick clickEvent =
        ImageClick.newBuilder().setThumbnailId("acct1_vid1_thumb1")
            .setIsImageClick(true).setPageCoords(new Coords(0.5f, 34.2f))
            .setWindowCoords(new Coords(0.4f, 3.4f)).build();
    TrackerEvent event =
        buildDefaultEvent().setEventType(EventType.IMAGE_CLICK)
            .setEventData(clickEvent).build();
    Event avroEvent = buildAvroEvent(event);

    // Test
    serializer.setEvent(avroEvent);

    // Check the puts
    assertEquals(serializer.getActions().size(), 0);

    // Check the increments
    assertEquals(serializer.getIncrements().size(), 2);
    assertIncrementFound("THUMBNAIL_TIMESTAMP_EVENT_COUNTS",
        "acct1_vid1_thumb1_2014-11-21T23", "evts", "ic", 1);
    assertIncrementFound("TIMESTAMP_THUMBNAIL_EVENT_COUNTS",
        "2014-11-21T23_acct1_vid1_thumb1", "evts", "ic", 1);
  }

  @Test
  public void testImageLoad() throws Exception {
    // Build the event
    ImageLoad loadEvent =
        ImageLoad.newBuilder().setThumbnailId("acct1_vid1_thumb1")
            .setHeight(600).setWidth(400).build();
    TrackerEvent event =
        buildDefaultEvent().setEventType(EventType.IMAGE_LOAD)
            .setEventData(loadEvent).build();
    Event avroEvent = buildAvroEvent(event);

    // Test
    serializer.setEvent(avroEvent);

    // Check the puts
    assertEquals(serializer.getActions().size(), 0);

    // Check the increments
    assertEquals(serializer.getIncrements().size(), 2);
    assertIncrementFound("THUMBNAIL_TIMESTAMP_EVENT_COUNTS",
        "acct1_vid1_thumb1_2014-11-21T23", "evts", "il", 1);
    assertIncrementFound("TIMESTAMP_THUMBNAIL_EVENT_COUNTS",
        "2014-11-21T23_acct1_vid1_thumb1", "evts", "il", 1);
  }

  @Test
  public void testImagesLoaded() throws Exception {
    // Build the event
    ImagesLoaded loadEvent =
        new ImagesLoaded(true, Arrays.asList(new ImageLoad("acct1_vid1_thumb1",
            600, 400), new ImageLoad("acct1_vid2_thumb2", 500, 300)));
    TrackerEvent event =
        buildDefaultEvent().setEventType(EventType.IMAGES_LOADED)
            .setEventData(loadEvent).build();
    Event avroEvent = buildAvroEvent(event);

    // Test
    serializer.setEvent(avroEvent);

    // Check the puts
    assertEquals(serializer.getActions().size(), 0);

    // Check the increments
    assertEquals(serializer.getIncrements().size(), 4);
    assertIncrementFound("THUMBNAIL_TIMESTAMP_EVENT_COUNTS",
        "acct1_vid1_thumb1_2014-11-21T23", "evts", "il", 1);
    assertIncrementFound("THUMBNAIL_TIMESTAMP_EVENT_COUNTS",
        "acct1_vid2_thumb2_2014-11-21T23", "evts", "il", 1);
    assertIncrementFound("TIMESTAMP_THUMBNAIL_EVENT_COUNTS",
        "2014-11-21T23_acct1_vid1_thumb1", "evts", "il", 1);
    assertIncrementFound("TIMESTAMP_THUMBNAIL_EVENT_COUNTS",
        "2014-11-21T23_acct1_vid2_thumb2", "evts", "il", 1);

  }

  @Test
  public void testEmptyThumbnailId() throws Exception {
    // Build the event
    ImageLoad loadEvent =
        ImageLoad.newBuilder().setThumbnailId("").setHeight(600).setWidth(400)
            .build();
    TrackerEvent event =
        buildDefaultEvent().setEventType(EventType.IMAGE_LOAD)
            .setEventData(loadEvent).build();
    Event avroEvent = buildAvroEvent(event);

    // Test
    serializer.setEvent(avroEvent);

    // Check the puts
    assertEquals(serializer.getActions().size(), 0);

    // Check the increments
    assertEquals(serializer.getIncrements().size(), 0);
    assertLogExists(Level.ERROR, "thumbnail id is empty");
  }

  @Test
  public void testEmptyThumbnailArray() throws Exception {
    // Build the event
    ImagesVisible visEvent =
        new ImagesVisible(true, new ArrayList<CharSequence>());
    TrackerEvent event =
        buildDefaultEvent().setEventType(EventType.IMAGES_VISIBLE)
            .setEventData(visEvent).build();
    Event avroEvent = buildAvroEvent(event);

    // Test
    serializer.setEvent(avroEvent);

    // Check the puts
    assertEquals(serializer.getActions().size(), 0);

    // Check the increments
    assertEquals(serializer.getIncrements().size(), 0);
  }

  @Test
  public void testAdPlay() throws Exception {
    // Build the event
    AdPlay adEvent =
        AdPlay.newBuilder().setAutoplayDelta(300).setPlayCount(1)
            .setPlayerId("playerid").setVideoId("acct1_vid1")
            .setThumbnailId("acct1_vid1_thumb_1").build();
    TrackerEvent event =
        buildDefaultEvent().setEventType(EventType.AD_PLAY)
            .setEventData(adEvent).build();
    Event avroEvent = buildAvroEvent(event);

    // Test
    serializer.setEvent(avroEvent);

    // Check the puts
    assertEquals(serializer.getActions().size(), 0);

    // Check the increments. Ad plays should be dropped for now
    assertEquals(serializer.getIncrements().size(), 0);
  }

  @Test
  public void testAdPlayNoThumb() throws Exception {
    // Build the event
    AdPlay adEvent =
        AdPlay.newBuilder().setAutoplayDelta(300).setPlayCount(1)
            .setPlayerId("playerid").setVideoId("acct1_vid1")
            .setThumbnailId(null).build();
    TrackerEvent event =
        buildDefaultEvent().setEventType(EventType.AD_PLAY)
            .setEventData(adEvent).build();
    Event avroEvent = buildAvroEvent(event);

    // Test
    serializer.setEvent(avroEvent);

    // Check the puts
    assertEquals(serializer.getActions().size(), 0);

    // Check the increments
    assertEquals(serializer.getIncrements().size(), 0);
  }

  @Test
  public void testVideoPlay() throws Exception {
    // Build the event
    VideoPlay vidEvent =
        VideoPlay.newBuilder().setAutoplayDelta(300).setPlayCount(1)
            .setPlayerId("playerid").setVideoId("acct1_vid1")
            .setThumbnailId("acct1_vid1_thumb_1").setDidAdPlay(true).build();
    TrackerEvent event =
        buildDefaultEvent().setEventType(EventType.VIDEO_PLAY)
            .setEventData(vidEvent).build();
    Event avroEvent = buildAvroEvent(event);

    // Test
    serializer.setEvent(avroEvent);

    // Check the puts
    assertEquals(serializer.getActions().size(), 0);

    // Check the increments. Video plays should be dropped for now
    assertEquals(serializer.getIncrements().size(), 0);
  }

  @Test
  public void testVideoViewingPercentage() throws Exception {
    // Build the event
    VideoViewPercentage viewEvent =
        VideoViewPercentage.newBuilder().setPercent(40.5f).setPlayCount(1)
            .setVideoId("acct1_vid1").build();
    TrackerEvent event =
        buildDefaultEvent().setEventType(EventType.VIDEO_VIEW_PERCENTAGE)
            .setEventData(viewEvent).build();
    Event avroEvent = buildAvroEvent(event);

    // Test
    serializer.setEvent(avroEvent);

    // Check the puts
    assertEquals(serializer.getActions().size(), 0);

    // Check the increments. We ignore video view percentages
    assertEquals(serializer.getIncrements().size(), 0);
  }

  @Test
  public void testEmptyTimestamp() throws Exception {
    // Build the event
    ImageLoad loadEvent =
        ImageLoad.newBuilder().setThumbnailId("acct1_vid1_thumb1")
            .setHeight(600).setWidth(400).build();
    TrackerEvent event =
        buildDefaultEvent().setEventType(EventType.IMAGE_LOAD)
            .setEventData(loadEvent).build();
    Event avroEvent = buildAvroEvent(event);
    avroEvent.getHeaders().put("timestamp", "");

    // Test
    // TODO(mdesnoyer): Assert error log exists
    serializer.setEvent(avroEvent);
    assertLogExists(Level.ERROR, "Unable to obtain timestamp header");

    // Check the puts
    assertEquals(serializer.getActions().size(), 0);

    // Check the increments
    assertEquals(serializer.getIncrements().size(), 0);
  }

  @Test
  public void testTimestampMissing() throws Exception {
    // Build the event
    ImageLoad loadEvent =
        ImageLoad.newBuilder().setThumbnailId("acct1_vid1_thumb1")
            .setHeight(600).setWidth(400).build();
    TrackerEvent event =
        buildDefaultEvent().setEventType(EventType.IMAGE_LOAD)
            .setEventData(loadEvent).build();
    Event avroEvent = buildAvroEvent(event);
    avroEvent.getHeaders().remove("timestamp");

    // Test
    serializer.setEvent(avroEvent);
    assertLogExists(Level.ERROR, "Unable to obtain timestamp header");

    // Check the puts
    assertEquals(serializer.getActions().size(), 0);

    // Check the increments
    assertEquals(serializer.getIncrements().size(), 0);
  }

  @Test
  public void testWrongSchemaForEvent() throws IOException {
    // Build the event
    ImageVisible visEvent =
        ImageVisible.newBuilder().setThumbnailId("acct1_vid1_thumb1").build();
    TrackerEvent event =
        buildDefaultEvent().setEventType(EventType.IMAGE_VISIBLE)
            .setEventData(visEvent).build();
    Event avroEvent = buildAvroEvent(event);

    // Mock out the schema at the url so that it's the wrong schema
    String badSchema =
        "{\"type\":\"record\", \"name\":\"TrackerEvent\", \"fields\":[{\"name\":\"pageUrl\", \"type\":\"string\"}]}";
    when(mockURL.open(VALID_SCHEMA_URL)).thenReturn(
        new ByteArrayInputStream(badSchema.getBytes()));
    
    serializer.setEvent(avroEvent);

    // Check the outputs
    assertEquals(serializer.getActions().size(), 0);
    assertEquals(serializer.getIncrements().size(), 0);
    
    assertLogExists(Level.ERROR, "Error parsing avro event");
  }
  
  @Test
  public void testWrongEncodedData() throws IOException {
    Schema badSchema = new Schema.Parser().parse(
        "{\"type\":\"record\", \"name\":\"TrackerEvent\", \"fields\":[{\"name\":\"serverTime\", \"type\":\"long\"}]}");
    GenericRecord record = new GenericRecordBuilder(badSchema).set("serverTime", 1416612478000L).build();
    
    Event event = buildAvroEvent(record, badSchema, VALID_SCHEMA_URL);
    
    // Mock out the schema on the server to be the tracker one
    when(mockURL.open(VALID_SCHEMA_URL)).thenReturn(
        new ByteArrayInputStream(TrackerEvent.getClassSchema().toString().getBytes()));
    
    serializer.setEvent(event);

    // Check the outputs
    assertEquals(serializer.getActions().size(), 0);
    assertEquals(serializer.getIncrements().size(), 0);
    
    assertLogExists(Level.ERROR, "Error reading avro event");
  }
  
  @Test
  public void testEventMissingFields() throws IOException {
    Schema badSchema = new Schema.Parser().parse(
        "{\"type\":\"record\", \"name\":\"TrackerEvent\", \"fields\":[{\"name\":\"serverTime\", \"type\":\"long\"}]}");
    GenericRecord record = new GenericRecordBuilder(badSchema).set("serverTime", 1416612478000L).build();
    
    Event event = buildAvroEvent(record, badSchema, VALID_SCHEMA_URL);
    
    serializer.setEvent(event);

    // Check the outputs
    assertEquals(serializer.getActions().size(), 0);
    assertEquals(serializer.getIncrements().size(), 0);
    
    assertLogExists(Level.ERROR, "Error parsing avro event");
  }

  @Test
  public void testConnectionErrorToGetSchema() throws IOException {
    // Build the event
    ImageVisible visEvent =
        ImageVisible.newBuilder().setThumbnailId("acct1_vid1_thumb1").build();
    TrackerEvent event =
        buildDefaultEvent().setEventType(EventType.IMAGE_VISIBLE)
            .setEventData(visEvent).build();
    Event avroEvent = buildAvroEvent(event);

    when(mockURL.open(VALID_SCHEMA_URL)).thenThrow(
        new IOException("Connection Error"));

    serializer.setEvent(avroEvent);

    // Event is dropped
    assertEquals(serializer.getActions().size(), 0);
    assertEquals(serializer.getIncrements().size(), 0);
    assertLogExists(Level.ERROR, "Unable to download the schema");
  }

  private TrackerEvent.Builder buildDefaultEvent() {
    return TrackerEvent
        .newBuilder()
        .setPageId(new Utf8("pageId_dummy"))
        .setTrackerAccountId("trackerAccountId_dummy")
        .setTrackerType(TrackerType.IGN)
        .setPageURL("pageUrl_dummy")
        .setRefURL("refUrl_dummy")
        .setServerTime(1416612478000L)
        .setClientTime(1416612478000L)
        .setClientIP("clientIp_dummy")
        .setNeonUserId("neonUserId_dummy")
        .setUserAgent("userAgentDummy")
        .setAgentInfo(null)
        .setIpGeoData(
            GeoData.newBuilder().setCity("Toronto").setCountry("CAN")
                .setZip(null).setRegion("ON").setLat(null).setLon(null).build());
  }

  private GenericRecordBuilder buildDefaultGenericEvent(Schema schema) {
    return new GenericRecordBuilder(schema)
        .set("pageId", new Utf8("pageId_dummy"))
        .set("trackerAccountId", "trackerAccountId_dummy")
        .set("trackerType", TrackerType.IGN)
        .set("pageURL", "pageUrl_dummy")
        .set("refURL", "refUrl_dummy")
        .set("serverTime", 1416612478000L)
        .set("clientTime", 1416612478000L)
        .set("clientIP", "clientIp_dummy")
        .set("neonUserId", "neonUserId_dummy")
        .set("userAgent", "userAgentDummy")
        .set("agentInfo", null)
        .set(
            "ipGeoData",
            GeoData.newBuilder().setCity("Toronto").setCountry("CAN")
                .setZip(null).setRegion("ON").setLat(null).setLon(null).build());
  }

  private Event buildAvroEvent(TrackerEvent event) throws IOException {
    return buildAvroEvent(event, event.getSchema(), VALID_SCHEMA_URL);

  }

  private Event buildAvroEvent(GenericRecord event, Schema schema,
      String schemaURL) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    GenericDatumWriter<GenericRecord> datumWriter =
        new GenericDatumWriter<GenericRecord>(schema);

    datumWriter.write(event, encoder);
    encoder.flush();
    byte[] encodedEvent = out.toByteArray();

    // make avro container headers
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("flume.avro.schema.url", schemaURL);
    headers.put("timestamp", event.get("serverTime").toString()); // milli
                                                                  // seconds

    // Mock out the request for this schema so that it is returned
    when(mockURL.open(schemaURL)).thenReturn(
        new ByteArrayInputStream(schema.toString().getBytes()));

    return EventBuilder.withBody(encodedEvent, headers);
  }

  private void assertIncrementFound(String table, String row,
      String col_family, String col, int amount) {
    for (AtomicIncrementRequest inc : serializer.getIncrements()) {
      if (table.equals(new String(inc.table()))
          && row.equals(new String(inc.key()))
          && col_family.equals(new String(inc.family()))
          && col.equals(new String(inc.qualifier()))
          && amount == inc.getAmount()) {
        // Found the increment
        return;
      }
    }
    fail("The expected increment (" + table + "," + row + "," + col_family
        + "," + col + " = " + amount + ")  was not found. We did see "
        + serializer.getIncrements().toString());
  }

  private void assertLogExists(Level level, String regex) {
    if (!testAppender.logExists(regex, level)) {
      fail("The expected log: " + regex + " was not found. Logs seen:\n"
          + testAppender.getLogs());
    }
  }
}

//TODO: Move this class into some common utilities if it is used again
class TestAppender extends AppenderSkeleton {
  private final List<LoggingEvent> captured_logs =
      new ArrayList<LoggingEvent>();

  @Override
  public boolean requiresLayout() {
    return false;
  }

  @Override
  protected void append(final LoggingEvent loggingEvent) {
    captured_logs.add(loggingEvent);
  }

  @Override
  public void close() {
  }

  /**
   * @param regex
   *          - Regex to see if the log exists
   * @param level
   *          - Level to search for the log event in
   * @return true if the log exists in what was captured
   */
  public boolean logExists(String regex, Level level) {
    Pattern pattern = Pattern.compile(regex);
    for (LoggingEvent event : captured_logs) {
      if (event.getLevel().equals(level)
          && pattern.matcher((CharSequence) event.getMessage()).find()) {
        return true;
      }
    }
    return false;
  }

  public String getLogs() {
    String retval = "";
    for (LoggingEvent event : captured_logs) {
      retval += event.getLevel().toString() + ": " + event.getMessage() + "\n";
    }
    return retval;
  }
}
