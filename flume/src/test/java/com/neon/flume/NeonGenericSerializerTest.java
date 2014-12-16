package com.neon.flume;

import  com.neon.Tracker.*;

import org.junit.* ;
import static org.junit.Assert.* ;

import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.util.Date;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.io.DatumWriter;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.SimpleHbaseEventSerializer.KeyType;
import org.apache.flume.sink.hbase.AsyncHbaseEventSerializer;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;


class NeonGenericSerializerTest {   
    
    public NeonGenericSerializerTest() {
        
    }
    
    @Test
    public void test_ImageVisible_Base() throws Exception { 
    
        Schema writerSchema = new TrackerEvent().getSchema();
        GenericData.Record trackerEvent = new GenericData.Record(writerSchema);
        
        trackerEvent.put("pageId", new Utf8("pageId_dummy"));
        trackerEvent.put("trackerAccountId", new Utf8("trackerAccountId_dummy"));
        //trackerEvent.put(com.neon.Tracker.TrackerType.IGN);
        
        GenericData.EnumSymbol trackerType = new GenericData.EnumSymbol(writerSchema, "IGN");
        trackerEvent.put("trackerType", trackerType);
        
        trackerEvent.put ("pageURL", new Utf8("pageUrl_dummy"));
        trackerEvent.put ("refURL", new Utf8("refUrl_dummy"));
        
        trackerEvent.put("serverTime", 1418689680L);
        trackerEvent.put("clientTime", 1418689680L);
        
        trackerEvent.put("clientIP", new Utf8("clientIp_dummy"));
        trackerEvent.put ("neonUserId", new Utf8("neonUserId_dummy"));
        trackerEvent.put("userAgent", new Utf8("userAgent_dummy"));
        
        GenericData.EnumSymbol eventType = new GenericData.EnumSymbol(writerSchema, "IMAGE_VISIBLE");
        trackerEvent.put("eventType", eventType);
        
        GenericData.Record agentInfo = new GenericData.Record(writerSchema);
        trackerEvent.put("agentInfo", null);
    
        Schema.Field geoDataField = writerSchema.getField("ipGeoData");
        GenericRecord geoData = new GenericData.Record(geoDataField.schema());
        geoData.put("country", new Utf8("usa"));
        trackerEvent.put("ipGeoData", geoData); 
        
        Schema.Field eventData = writerSchema.getField("eventData");
        Schema eventDataSchema = eventData.schema();
        int i = eventDataSchema.getIndexNamed("com.neon.Tracker.ImageVisible");
        GenericRecord img = new GenericData.Record(eventDataSchema.getTypes().get(i));
        img.put("thumbnailId", new Utf8("test_ImageVisible_Base"));
        trackerEvent.put("eventData", img); 
        
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(writerSchema);
            
        datumWriter.write(trackerEvent, encoder);
        encoder.flush();

        byte[] encodedEvent = out.toByteArray();

        // make avro container headers
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("flume.avro.schema.url"," https://s3.amazonaws.com/neon-avro-schema/3325be34d95af2ca7d2db2b327e93408.avsc" );
        headers.put("timestamp", "1416612478000");  // milli seconds

        Event event = EventBuilder.withBody(encodedEvent, headers);
        NeonGenericSerializer serializer = new NeonGenericSerializer();

        String table = "table";
        String columnFamily = "columFamily";
        serializer.initialize(table.getBytes(), columnFamily.getBytes());

        /*
        *   Test 
        */
        serializer.setEvent(event);
        
        /*
        *   Test 
        */
        List<PutRequest> puts = serializer.getActions();
        
        // should be zero size
        if(puts.size() != 0) 
            throw new Exception("");
        
        /*
        *   Test 
        */
        long timestamp = 1416612478000L;
        Date date = new Date(timestamp);
        DateFormat format = new SimpleDateFormat("YYYY-MM-dd'T'HH");
        byte[] formattedTimestamp = format.format(date).getBytes();
        String eventTimestamp = new String(formattedTimestamp);
    
        List<AtomicIncrementRequest> incs = serializer.getIncrements();
        
        // should be zero size
        if(incs.size() != 2) 
            throw new Exception("");
    }

    @Test
    public void test_ImageVisible_New_Field() throws Exception { 

        String schemaUrl = "https://s3.amazonaws.com/neon-test/test_tracker_event_schema_added_field.avsc";
        Schema writerSchema = loadFromUrl(schemaUrl);
        GenericData.Record trackerEvent = new GenericData.Record(writerSchema);
        
        trackerEvent.put("pageId", new Utf8("pageId_dummy"));
        trackerEvent.put("trackerAccountId", new Utf8("trackerAccountId_dummy"));
        //trackerEvent.put(com.neon.Tracker.TrackerType.IGN);
        
        GenericData.EnumSymbol trackerType = new GenericData.EnumSymbol(writerSchema, "IGN");
        trackerEvent.put("trackerType", trackerType);
        
        trackerEvent.put ("pageURL", new Utf8("pageUrl_dummy"));
        trackerEvent.put ("refURL", new Utf8("refUrl_dummy"));
        trackerEvent.put ("dummyNewField", new Utf8("dummy"));
        
        trackerEvent.put("serverTime", 1000L);
        trackerEvent.put("clientTime", 1000L);
        
        trackerEvent.put("clientIP", new Utf8("clientIp_dummy"));
        trackerEvent.put ("neonUserId", new Utf8("neonUserId_dummy"));
        trackerEvent.put("userAgent", new Utf8("userAgent_dummy"));
        
        GenericData.EnumSymbol eventType = new GenericData.EnumSymbol(writerSchema, "IMAGE_VISIBLE");
        trackerEvent.put("eventType", eventType);
        
        GenericData.Record agentInfo = new GenericData.Record(writerSchema);
        trackerEvent.put("agentInfo", null);
    
        Schema.Field geoDataField = writerSchema.getField("ipGeoData");
        GenericRecord geoData = new GenericData.Record(geoDataField.schema());
        geoData.put("country", new Utf8("usa"));
        trackerEvent.put("ipGeoData", geoData); 
        
        Schema.Field eventData = writerSchema.getField("eventData");
        Schema eventDataSchema = eventData.schema();
        int i = eventDataSchema.getIndexNamed("com.neon.Tracker.ImageVisible");
        GenericRecord img = new GenericData.Record(eventDataSchema.getTypes().get(i));
        img.put("thumbnailId", new Utf8("test_ImageVisible_New_Field"));
        trackerEvent.put("eventData", img); 
        
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(writerSchema);
            
        datumWriter.write(trackerEvent, encoder);
        encoder.flush();

        byte[] encodedEvent = out.toByteArray();

        // make avro container headers
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("flume.avro.schema.url",schemaUrl );
        headers.put("timestamp", "1416612478000");  // milli seconds

        Event event = EventBuilder.withBody(encodedEvent, headers);
        NeonGenericSerializer serializer = new NeonGenericSerializer();

        String table = "table";
        String columnFamily = "columFamily";
        serializer.initialize(table.getBytes(), columnFamily.getBytes());

        serializer.setEvent(event);
        List<PutRequest> puts = serializer.getActions();
        List<AtomicIncrementRequest> incs =serializer.getIncrements();

    }

    @Test
    public void test_ImageVisible_New_Field_in_EventData() throws Exception { 

        String schemaUrl = "https://s3.amazonaws.com/neon-test/test_tracker_event_schema_added_event_data_record.avsc";
        Schema writerSchema = loadFromUrl(schemaUrl);
        GenericData.Record trackerEvent = new GenericData.Record(writerSchema);
        
        trackerEvent.put("pageId", new Utf8("pageId_dummy"));
        trackerEvent.put("trackerAccountId", new Utf8("trackerAccountId_dummy"));
        //trackerEvent.put(com.neon.Tracker.TrackerType.IGN);
        
        GenericData.EnumSymbol trackerType = new GenericData.EnumSymbol(writerSchema, "IGN");
        trackerEvent.put("trackerType", trackerType);
        
        trackerEvent.put ("pageURL", new Utf8("pageUrl_dummy"));
        trackerEvent.put ("refURL", new Utf8("refUrl_dummy"));
        
        trackerEvent.put("serverTime", 1000L);
        trackerEvent.put("clientTime", 1000L);
        
        trackerEvent.put("clientIP", new Utf8("clientIp_dummy"));
        trackerEvent.put ("neonUserId", new Utf8("neonUserId_dummy"));
        trackerEvent.put("userAgent", new Utf8("userAgent_dummy"));
        
        GenericData.EnumSymbol eventType = new GenericData.EnumSymbol(writerSchema, "IMAGE_VISIBLE");
        trackerEvent.put("eventType", eventType);
        
        GenericData.Record agentInfo = new GenericData.Record(writerSchema);
        trackerEvent.put("agentInfo", null);
    
        Schema.Field geoDataField = writerSchema.getField("ipGeoData");
        GenericRecord geoData = new GenericData.Record(geoDataField.schema());
        geoData.put("country", new Utf8("usa"));
        trackerEvent.put("ipGeoData", geoData); 
        
        Schema.Field eventData = writerSchema.getField("eventData");
        Schema eventDataSchema = eventData.schema();
        int i = eventDataSchema.getIndexNamed("com.neon.Tracker.ImageVisible");
        GenericRecord img = new GenericData.Record(eventDataSchema.getTypes().get(i));
        img.put("thumbnailId", new Utf8("test_ImageVisible_New_Field_in_EventData"));
        trackerEvent.put("eventData", img); 
        
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(writerSchema);
            
        datumWriter.write(trackerEvent, encoder);
        encoder.flush();

        byte[] encodedEvent = out.toByteArray();

        // make avro container headers
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("flume.avro.schema.url",schemaUrl );
        headers.put("timestamp", "1416612478000");  // milli seconds

        Event event = EventBuilder.withBody(encodedEvent, headers);
        NeonGenericSerializer serializer = new NeonGenericSerializer();

        String table = "table";
        String columnFamily = "columFamily";
        serializer.initialize(table.getBytes(), columnFamily.getBytes());

        serializer.setEvent(event);
        List<PutRequest> puts = serializer.getActions();
        List<AtomicIncrementRequest> incs =serializer.getIncrements();

    }

    private static Schema loadFromUrl(String schemaUrl) throws IOException {
        Schema.Parser parser = new Schema.Parser();
        InputStream is = null;
        try {
            is = new URL(schemaUrl).openStream();
            return parser.parse(is);
        } finally {
            if (is != null) {
                is.close();
            }
        }
    }
    
    public static void main(String[] args) {
        System.out.println("\n\nTest Starting"); 

        try {
            NeonGenericSerializerTest serializer = new NeonGenericSerializerTest();
            
            // features testing
            serializer.test_ImageVisible_Base();
            serializer.test_ImageVisible_New_Field();
            serializer.test_ImageVisible_New_Field_in_EventData();
            
            System.out.println("\n\nTest successful");
        }
        catch(IOException e) {
            System.out.println("Test failure: io exception: " + e.toString());
        }
        catch(Exception e) {
            System.out.println("Test failure: exception: " + e.toString());
        }
    
    }
}






