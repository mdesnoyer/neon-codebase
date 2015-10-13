package com.neon.flume;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventSerializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.neon.Tracker.GeoData;
import com.neon.Tracker.ImagesVisible;
import com.neon.Tracker.TrackerEvent;

public class TestNeonAvroEventSerializer {

  private File file;
  private Schema[] schemaArray = new Schema[10];
  private GenericRecord[] origRecord = new GenericRecord[10];

  private static TestAppender testAppender;
  private static Logger logger;

  @Before
  public void setUp() throws Exception {
    // Add the logger
    testAppender = new TestAppender();
    logger = Logger.getLogger(NeonAvroEventSerializer.class.getName());
    logger.addAppender(testAppender);

    file = File.createTempFile(getClass().getSimpleName(), "");
  }

  @After
  public void tearDown() {
    // Remove the logger
    logger.removeAppender(testAppender);
  }

  @Test
  public void testOneSchema() throws IOException {
    // Test to ensure that the serializer works in the most basic use case

    Schema schema = null;
    GenericRecord record = null;

    schema = TrackerEvent.getClassSchema();

    ImagesVisible visEvent = new ImagesVisible(true,
        Arrays.asList((CharSequence) "acct1_vid1_thumb1", "acct1_vid2_thumb2"));

    record = buildDefaultGenericEvent(schema).set("eventType", "IMAGE_VISIBLE").set("eventData", visEvent).build();

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();

    File schemaFile = File.createTempFile(getClass().getSimpleName(), ".avsc");
    Files.write(schema.toString(), schemaFile, Charsets.UTF_8);

    EventSerializer serializer = createEventSerializer(outStream);
    serializer.afterCreate();

    for (int i = 0; i < 10; i++) {
      Event event = EventBuilder.withBody(serializeAvro(record, schema));
      event.getHeaders().put(NeonAvroEventSerializer.AVRO_SCHEMA_URL_HEADER,
          schemaFile.toURI().toURL().toExternalForm());
      serializer.write(event);
      schemaArray[i] = schema;
      origRecord[i] = record;
    }

    shutDownAll(serializer, outStream);
    validateAvroFile(outStream, origRecord, schemaArray);
  }

  @Test
  public void testSchemaEvolution() throws IOException {
    // Test to ensure that the serializer works with multiple schemas
    // The schemas adhere to the rules of schema evolution
    OutputStream out = new FileOutputStream(file);

    EventSerializer serializer = createEventSerializer(out);
    serializer.afterCreate();

    ImagesVisible visEvent = new ImagesVisible(true,
        Arrays.asList((CharSequence) "acct1_vid1_thumb1", "acct1_vid2_thumb2"));

    Schema schema = null;
    GenericRecord record = null;
    JSONObject schemaJson = null;

    for (int i = 0; i < 10; i++) {
      if (i < 3 && i > 8) {
        schemaJson = new JSONObject(TrackerEvent.getClassSchema().toString());
        schemaJson.getJSONArray("fields").put(
            new JSONObject("{\"name\": \"dummyField\", \"type\" : [ \"null\" , \"int\" ], \"default\" : \"null\"}"));

        schema = new Schema.Parser().parse(schemaJson.toString());

        record = buildDefaultGenericEvent(schema).set("dummyField", 78).set("eventType", "IMAGE_VISIBLE")
            .set("eventData", visEvent).build();
      } else {
        schemaJson = new JSONObject(TrackerEvent.getClassSchema().toString());
        schema = new Schema.Parser().parse(schemaJson.toString());

        record = buildDefaultGenericEvent(schema).set("eventType", "IMAGE_VISIBLE").set("eventData", visEvent).build();
      }

      File schemaFile = File.createTempFile(getClass().getSimpleName(), ".avsc");
      Files.write(schema.toString(), schemaFile, Charsets.UTF_8);

      Event event = EventBuilder.withBody(serializeAvro(record, schema));
      event.getHeaders().put(NeonAvroEventSerializer.AVRO_SCHEMA_URL_HEADER,
          schemaFile.toURI().toURL().toExternalForm());
      serializer.write(event);
      schemaArray[i] = schema;
      origRecord[i] = record;
    }

    shutDownAll(serializer, out);
    validateAvroFile(file, origRecord);
  }

  @Test
  public void testSchemaNoEvolution() throws IOException {
    // Test to ensure that the serializer works with multiple schemas
    // The schemas do not adhere to the rules of schema evolution
    OutputStream out = new FileOutputStream(file);

    EventSerializer serializer = createEventSerializer(out);
    serializer.afterCreate();

    ImagesVisible visEvent = new ImagesVisible(true,
        Arrays.asList((CharSequence) "acct1_vid1_thumb1", "acct1_vid2_thumb2"));

    Schema schema = null;
    GenericRecord record = null;
    JSONObject schemaJson = null;

    FileInputStream JSONFile = new FileInputStream(new File("src/test/java/com/neon/flume/TestEvent.avsc"));

    String JSONFileStr = IOUtils.toString(JSONFile, "UTF-8");

    for (int i = 0; i < 10; i++) {
      if (i < 4) {
        schemaJson = new JSONObject(JSONFileStr);
      } else {
        schemaJson = new JSONObject(TrackerEvent.getClassSchema().toString());
      }

      schema = new Schema.Parser().parse(schemaJson.toString());

      record = buildDefaultGenericEvent(schema).set("eventType", "IMAGE_VISIBLE").set("eventData", visEvent).build();

      File schemaFile = File.createTempFile(getClass().getSimpleName(), ".avsc");
      Files.write(schema.toString(), schemaFile, Charsets.UTF_8);

      Event event = EventBuilder.withBody(serializeAvro(record, schema));
      event.getHeaders().put(NeonAvroEventSerializer.AVRO_SCHEMA_URL_HEADER,
          schemaFile.toURI().toURL().toExternalForm());
      serializer.write(event);
      schemaArray[i] = schema;
      origRecord[i] = record;
    }

    shutDownAll(serializer, out);
    validateAvroFile(file, origRecord);
  }

  @Test
  public void testBadSchema() throws IOException {
    // Test to ensure that the serializer fails properly when no schema is given
    OutputStream out = new FileOutputStream(file);

    EventSerializer serializer = createEventSerializer(out);
    serializer.afterCreate();

    Schema schema = null;
    GenericRecord record = null;

    String badSchema = "{\"type\":\"record\", \"name\":\"com.neon.Tracker.TrackerEvent\", \"fields\":[{\"name\":\"pageUrl\", \"type\":\"string\"}]}";

    schema = new Schema.Parser().parse(badSchema);

    record = new GenericRecordBuilder(schema).set("pageUrl", "Yes").build();

    File schemaFile = File.createTempFile(getClass().getSimpleName(), ".avsc");
    Files.write(schema.toString(), schemaFile, Charsets.UTF_8);

    for (int i = 0; i < 10; i++) {
      Event event = EventBuilder.withBody(serializeAvro(record, schema));
      event.getHeaders().put(NeonAvroEventSerializer.AVRO_SCHEMA_URL_HEADER,
          schemaFile.toURI().toURL().toExternalForm());
      serializer.write(event);
      assertLogExists(Level.ERROR,
          "Error parsing avro event " + "org.apache.avro.AvroTypeException: Found "
              + "com.neon.Tracker.TrackerEvent, expecting "
              + "com.neon.Tracker.TrackerEvent, missing required field pageId");
    }
    shutDownAll(serializer, out);
  }

  @Test
  public void testConnectionError() throws IOException {
    // Test to ensure that the serializer fails gracefully when a connection
    // error occurs

    Schema schema = null;
    GenericRecord record = null;

    schema = TrackerEvent.getClassSchema();

    ImagesVisible visEvent = new ImagesVisible(true,
        Arrays.asList((CharSequence) "acct1_vid1_thumb1", "acct1_vid2_thumb2"));

    record = buildDefaultGenericEvent(schema).set("eventType", "IMAGE_VISIBLE").set("eventData", visEvent).build();

    // serialize a few events using the reflection-based avro serializer
    OutputStream out = new FileOutputStream(file);

    File schemaFile = File.createTempFile(getClass().getSimpleName(), ".avsc");
    Files.write(schema.toString(), schemaFile, Charsets.UTF_8);

    EventSerializer serializer = createEventSerializer(out);
    serializer.afterCreate();

    Event event = EventBuilder.withBody(serializeAvro(record, schema));
    event.getHeaders().put("asd", schemaFile.toURI().toURL().toExternalForm());
    serializer.write(event);

    assertLogExists(Level.ERROR, "Error while writing Avro Event");
    shutDownAll(serializer, out);
  }

  public EventSerializer createEventSerializer(OutputStream out) {
    Context ctx = new Context();
    EventSerializer.Builder builder = new NeonAvroEventSerializer.Builder();
    EventSerializer serializer = builder.build(ctx, out);
    return serializer;
  }

  public void shutDownAll(EventSerializer serializer, OutputStream out) throws IOException {
    serializer.beforeClose();
    serializer.flush();
    out.flush();
    out.close();
  }

  private GenericRecordBuilder buildDefaultGenericEvent(Schema schema) {
    return new GenericRecordBuilder(schema).set("pageId", new Utf8("pageId_dummy"))
        .set("trackerAccountId", "trackerAccountId_dummy").set("trackerType", "IGN").set("pageURL", "pageUrl_dummy")
        .set("refURL", "refUrl_dummy").set("serverTime", 1416612478000L).set("clientTime", 1416612478000L)
        .set("clientIP", "clientIp_dummy").set("neonUserId", "neonUserId_dummy").set("userAgent", "userAgentDummy")
        .set("agentInfo", null).set("ipGeoData", GeoData.newBuilder().setCity("Toronto").setCountry("CAN").setZip(null)
            .setRegion("ON").setLat(null).setLon(null).build());
  }

  private byte[] serializeAvro(Object datum, Schema schema) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ReflectDatumWriter<Object> writer = new ReflectDatumWriter<Object>(schema);
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    out.reset();
    writer.write(datum, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  public void validateAvroFile(File file, GenericRecord origRecord[]) throws IOException {
    // read the events back using GenericRecord
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(file, reader);
    int numEvents = 0;
    while (fileReader.hasNext()) {
      GenericRecord record = new GenericData.Record(schemaArray[numEvents]);
      Assert.assertTrue(fileReader.next(record).toString().startsWith(origRecord[numEvents].toString()));
      numEvents++;
    }
    fileReader.close();
    Assert.assertEquals("Should have found a total of 10 events", 10, numEvents);
  }

  public void validateAvroFile(ByteArrayOutputStream out, GenericRecord origRecord[], Schema schemaArray[])
      throws IOException {
    byte buf[] = out.toByteArray();
    byte copy[] = Arrays.copyOfRange(buf, 35, buf.length - 1);
    ByteArrayInputStream recordReader = new ByteArrayInputStream(copy);
    int numEvents = 0;
    int end = 0;
    int offset = 0;
    int len = 20;
    String finale = new String(copy, "UTF-8");
    System.out.println(finale);
    while (numEvents < 10) {
      offset += len;
      DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schemaArray[numEvents]);
      Decoder decoder = DecoderFactory.get().binaryDecoder(recordReader, null);
      GenericRecord record = reader.read(null, decoder);
      // System.out.println(origRecord[numEvents].toString());
      // Assert.assertTrue(record.toString().startsWith(origRecord[numEvents].toString()));
      numEvents++;
    }
    Assert.assertEquals("Should have found a total of 10 events", 10, numEvents);
  }

  private void assertLogExists(Level level, String regex) {
    if (!testAppender.logExists(regex, level)) {
      fail("The expected log: " + regex + " was not found. Logs seen:\n" + testAppender.getLogs());
    }
  }
}
