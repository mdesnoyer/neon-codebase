package com.neon.flume;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.neon.Tracker.EventType;
import com.neon.Tracker.GeoData;
import com.neon.Tracker.ImagesVisible;
import com.neon.Tracker.TrackerEvent;
import com.neon.Tracker.TrackerType;

import java.io.ByteArrayOutputStream;
import java.io.File;
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
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventSerializer;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NeonTestAvroEventSerializer {

  private File file;

  @Before
  public void setUp() throws Exception {
    file = File.createTempFile(getClass().getSimpleName(), "");
  }

  @Test
  public void testNoCompression() throws IOException {
    createAvroFile(file, null, false);
    validateAvroFile(file);
  }

  @Test
  public void testNullCompression() throws IOException {
    createAvroFile(file, "null", false);
    validateAvroFile(file);
  }

  @Test
  public void testDeflateCompression() throws IOException {
    createAvroFile(file, "deflate", false);
    validateAvroFile(file);
  }

  @Test
  public void testSnappyCompression() throws IOException {
    createAvroFile(file, "snappy", false);
    validateAvroFile(file);
  }

  @Test
  public void testSchemaUrl() throws IOException {
    createAvroFile(file, null, true);
    validateAvroFile(file);
  }

  public void createAvroFile(File file, String codec, boolean useSchemaUrl) throws
      IOException {

    // serialize a few events using the reflection-based avro serializer
    OutputStream out = new FileOutputStream(file);

    Context ctx = new Context();
    if (codec != null) {
      ctx.put("compressionCodec", codec);
    }

    Schema[] schemaArray = new Schema[3];
    File schemaFile = null;
    File[] schemaFileArray = new File[3];
    Schema schema = null;
    
    for(int i = 0; i < 3; i++){
    	JSONObject schemaJson = null;
    	if(i == 1){
    		schemaJson =
    		        new JSONObject(TrackerEvent.getClassSchema().toString());
    	} else {
    		schemaJson =
    		        new JSONObject(TrackerEvent.getClassSchema().toString());
    		    schemaJson
    		        .getJSONArray("fields")
    		        .put(
    		            new JSONObject(//For some reason \"null\" raises errors, but \nnull does not. They're both supposed to represent a
    		            			   //default value of null, but only the second one works.
    		                "{\"name\": \"dummyField\", \"type\" : [ \"null\" , \"int\" ], \"default\" : \nnull}"));
    	}
    	schema = new Schema.Parser().parse(schemaJson.toString());
        schemaFile = null;
        if (useSchemaUrl) {
          schemaFile = File.createTempFile(getClass().getSimpleName(), ".avsc");
          Files.write(schema.toString(), schemaFile, Charsets.UTF_8);
          schemaFileArray[i] = schemaFile;
        }
        schemaArray[i] = schema;
    }
    
    EventSerializer.Builder builder = new NeonAvroEventSerializer.Builder();
    EventSerializer serializer = builder.build(ctx, out);

    serializer.afterCreate();
    for (int i = 0; i < 3; i++) {
        ImagesVisible visEvent =
            new ImagesVisible(true, Arrays.asList(
                (CharSequence) "acct1_vid1_thumb1", "acct1_vid2_thumb2"));

        GenericRecord record = null;
        if(i == 1){
        	record  =
                buildDefaultGenericEvent(schemaArray[i])
                    .set("eventType", EventType.IMAGE_VISIBLE)
                    .set("eventData", visEvent).build();
        } else {
        	record = buildDefaultGenericEvent(schemaArray[i]).set("dummyField", null)
        			.set("eventType", EventType.IMAGE_VISIBLE)
        			.set("eventData", visEvent).build();
        }
      Event event = EventBuilder.withBody(serializeAvro(record, schemaArray[i]));
      
      if (schemaFile == null) {
        event.getHeaders().put(NeonAvroEventSerializer.AVRO_SCHEMA_LITERAL_HEADER,
            schemaArray[i].toString());
      } else {
        event.getHeaders().put(NeonAvroEventSerializer.AVRO_SCHEMA_URL_HEADER,
            schemaFileArray[i].toURI().toURL().toExternalForm());
      }
      serializer.write(event);

    }
    
    serializer.flush();
    serializer.beforeClose();
    out.flush();
    out.close();
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
  
  private byte[] serializeAvro(Object datum, Schema schema) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ReflectDatumWriter<Object> writer = new ReflectDatumWriter<Object>(schema);
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    out.reset();
    writer.write(datum, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  public void validateAvroFile(File file) throws IOException {
    // read the events back using GenericRecord
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> fileReader =
        new DataFileReader<GenericRecord>(file, reader);
    GenericRecord record = new GenericData.Record(fileReader.getSchema());
    int numEvents = 0;
    while (fileReader.hasNext()) {
      fileReader.next(record);
      numEvents++;
    }
    fileReader.close();
    Assert.assertEquals("Should have found a total of 3 events", 3, numEvents);
  }
  
}
