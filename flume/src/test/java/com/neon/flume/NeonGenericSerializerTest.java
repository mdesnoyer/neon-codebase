package com.neon.flume;

import  com.neon.Tracker.*;

import org.junit.* ;
import static org.junit.Assert.* ;

import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.avro.generic.*;
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


public class NeonGenericSerializerTest {   
    
    public NeonGenericSerializerTest() {
        
    }
    
    @Test
    public void test_ImageVisible_Base() throws Exception { 
        
        String videoId = "test_ImageVisible_Base";
    
        Schema writerSchema = new TrackerEvent().getSchema();
        GenericData.Record trackerEvent = new GenericData.Record(writerSchema);
        
        dummyFill(trackerEvent, writerSchema);
        
        GenericData.EnumSymbol eventType = new GenericData.EnumSymbol(writerSchema, "IMAGE_VISIBLE");
        trackerEvent.put("eventType", eventType);
        
        Schema.Field eventData = writerSchema.getField("eventData");
        Schema eventDataSchema = eventData.schema();
        int i = eventDataSchema.getIndexNamed("com.neon.Tracker.ImageVisible");
        GenericRecord img = new GenericData.Record(eventDataSchema.getTypes().get(i));
        img.put("thumbnailId", new Utf8(videoId));
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

        // Test
        serializer.setEvent(event);
        
        // Test
        List<PutRequest> puts = serializer.getActions();
        // should be zero size
        assertTrue(puts.size() == 0); 
        
        // Test 
        long timestamp = 1416612478000L;
        Date date = new Date(timestamp);
        DateFormat format = new SimpleDateFormat("YYYY-MM-dd'T'HH");
        byte[] formattedTimestamp = format.format(date).getBytes();
        String eventTimestamp = new String(formattedTimestamp);
        
        List<AtomicIncrementRequest> incs = serializer.getIncrements();
        
        assertTrue(incs.size() == 2);
        
        AtomicIncrementRequest req = incs.get(0);
        String key = videoId + "_" + eventTimestamp;
        assertTrue(Arrays.equals(req.key(), key.getBytes()));
        assertTrue(req.getAmount() == 1);
        
        req = incs.get(1);
        key = eventTimestamp + "_" + videoId;
        assertTrue(Arrays.equals(req.key(), key.getBytes()));
        assertTrue(req.getAmount() == 1);
    }

    @Test
    public void test_ImageVisible_New_Field() throws Exception { 

        String videoId = "test_ImageVisible_New_Field";

        String schemaUrl = "https://s3.amazonaws.com/neon-test/test_tracker_event_schema_added_field.avsc";
        Schema writerSchema = loadFromUrl(schemaUrl);
        GenericData.Record trackerEvent = new GenericData.Record(writerSchema);
        
        dummyFill(trackerEvent, writerSchema);
        
        GenericData.EnumSymbol eventType = new GenericData.EnumSymbol(writerSchema, "IMAGE_VISIBLE");
        trackerEvent.put("eventType", eventType);
        
        // test
        trackerEvent.put ("dummyNewField", new Utf8("dummyNewField"));
        
        Schema.Field eventData = writerSchema.getField("eventData");
        Schema eventDataSchema = eventData.schema();
        int i = eventDataSchema.getIndexNamed("com.neon.Tracker.ImageVisible");
        GenericRecord img = new GenericData.Record(eventDataSchema.getTypes().get(i));
        img.put("thumbnailId", new Utf8(videoId));
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

        // Test 
        serializer.setEvent(event);
    
        // Test
        List<PutRequest> puts = serializer.getActions();
        // should be zero size
        assertTrue(puts.size() == 0); 
        
        // Test 
        long timestamp = 1416612478000L;
        Date date = new Date(timestamp);
        DateFormat format = new SimpleDateFormat("YYYY-MM-dd'T'HH");
        byte[] formattedTimestamp = format.format(date).getBytes();
        String eventTimestamp = new String(formattedTimestamp);
        
        List<AtomicIncrementRequest> incs = serializer.getIncrements();
        
        assertTrue(incs.size() == 2);
        
        AtomicIncrementRequest req = incs.get(0);
        String key = videoId + "_" + eventTimestamp;
        assertTrue(Arrays.equals(req.key(), key.getBytes()));
        assertTrue(req.getAmount() == 1);
        
        req = incs.get(1);
        key = eventTimestamp + "_" + videoId;
        assertTrue(Arrays.equals(req.key(), key.getBytes()));
        assertTrue(req.getAmount() == 1);
    }

    @Test
    public void test_ImageVisible_New_Field_in_EventData() throws Exception { 

        String videoId = "test_ImageVisible_New_Field_in_EventData";

        String schemaUrl = "https://s3.amazonaws.com/neon-test/test_tracker_event_schema_added_event_data_record.avsc";
        Schema writerSchema = loadFromUrl(schemaUrl);
        GenericData.Record trackerEvent = new GenericData.Record(writerSchema);
        
        dummyFill(trackerEvent, writerSchema);
        
        GenericData.EnumSymbol eventType = new GenericData.EnumSymbol(writerSchema, "IMAGE_VISIBLE");
        trackerEvent.put("eventType", eventType);
         
        Schema.Field eventData = writerSchema.getField("eventData");
        Schema eventDataSchema = eventData.schema();
        int i = eventDataSchema.getIndexNamed("com.neon.Tracker.ImageVisible");
        GenericRecord img = new GenericData.Record(eventDataSchema.getTypes().get(i));
        img.put("thumbnailId", new Utf8(videoId));
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

        // Test 
        serializer.setEvent(event);
    
        // Test
        List<PutRequest> puts = serializer.getActions();
        // should be zero size
        assertTrue(puts.size() == 0); 
        
        // Test 
        long timestamp = 1416612478000L;
        Date date = new Date(timestamp);
        DateFormat format = new SimpleDateFormat("YYYY-MM-dd'T'HH");
        byte[] formattedTimestamp = format.format(date).getBytes();
        String eventTimestamp = new String(formattedTimestamp);
        
        List<AtomicIncrementRequest> incs = serializer.getIncrements();
        
        assertTrue(incs.size() == 2);
        
        AtomicIncrementRequest req = incs.get(0);
        String key = videoId + "_" + eventTimestamp;
        assertTrue(Arrays.equals(req.key(), key.getBytes()));
        assertTrue(req.getAmount() == 1);
        
        req = incs.get(1);
        key = eventTimestamp + "_" + videoId;
        assertTrue(Arrays.equals(req.key(), key.getBytes()));
        assertTrue(req.getAmount() == 1);
    }
    
    @Test
    public void test_ImagesVisible() throws Exception { 
        
        String videoId_1 = "test_ImageVisibles_1";
        String videoId_2 = "test_ImageVisibles_2";
    
        Schema writerSchema = new TrackerEvent().getSchema();
        GenericData.Record trackerEvent = new GenericData.Record(writerSchema);
        
        dummyFill(trackerEvent, writerSchema);
        
        GenericData.EnumSymbol eventType = new GenericData.EnumSymbol(writerSchema, "IMAGES_VISIBLE");
        trackerEvent.put("eventType", eventType);
        
        Schema.Field eventData = writerSchema.getField("eventData");
        Schema eventDataSchema = eventData.schema();
        int i = eventDataSchema.getIndexNamed("com.neon.Tracker.ImagesVisible");
        Schema imgSchema = eventDataSchema.getTypes().get(i);
        GenericRecord img = new GenericData.Record(imgSchema);
        
        Schema.Field thumbs = imgSchema.getField("thumbnailIds");
        GenericArray<String> values = new GenericData.Array<String>(2, thumbs.schema());
        
        values.add(0, videoId_1);
        values.add(1, videoId_2);
        img.put("thumbnailIds", values);
        img.put("isImagesVisible", true);
        
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

        // Test
        serializer.setEvent(event);
        
        // Test
        List<PutRequest> puts = serializer.getActions();
        // should be zero size
        assertTrue(puts.size() == 0); 
        
        // Test 
        long timestamp = 1416612478000L;
        Date date = new Date(timestamp);
        DateFormat format = new SimpleDateFormat("YYYY-MM-dd'T'HH");
        byte[] formattedTimestamp = format.format(date).getBytes();
        String eventTimestamp = new String(formattedTimestamp);
        
        List<AtomicIncrementRequest> incs = serializer.getIncrements();
        
        assertTrue(incs.size() == 4);
        
        AtomicIncrementRequest req = incs.get(0);
        String key = videoId_1 + "_" + eventTimestamp;
        assertTrue(Arrays.equals(req.key(), key.getBytes()));
        assertTrue(req.getAmount() == 1);
        
        req = incs.get(1);
        key = eventTimestamp + "_" + videoId_1;
        assertTrue(Arrays.equals(req.key(), key.getBytes()));
        assertTrue(req.getAmount() == 1);
        
        req = incs.get(2);
        key = videoId_2 + "_" + eventTimestamp;
        assertTrue(Arrays.equals(req.key(), key.getBytes()));
        assertTrue(req.getAmount() == 1);
        
        req = incs.get(3);
        key = eventTimestamp + "_" + videoId_2;
        assertTrue(Arrays.equals(req.key(), key.getBytes()));
        assertTrue(req.getAmount() == 1);
        
        
    }
    
    

    @Test
    public void test_ImageClick() throws Exception { 
        
        String videoId = "test_ImageClick";
    
        Schema writerSchema = new TrackerEvent().getSchema();
        GenericData.Record trackerEvent = new GenericData.Record(writerSchema);
        
        dummyFill(trackerEvent, writerSchema);
        
        GenericData.EnumSymbol eventType = new GenericData.EnumSymbol(writerSchema, "IMAGE_CLICK");
        trackerEvent.put("eventType", eventType);
        
        Schema.Field eventData = writerSchema.getField("eventData");
        Schema eventDataSchema = eventData.schema();
        int i = eventDataSchema.getIndexNamed("com.neon.Tracker.ImageClick");
        Schema imgSchema = eventDataSchema.getTypes().get(i);
        GenericRecord img = new GenericData.Record(imgSchema);
        
        //img.put("thumbnailId", new Utf8(videoId));
        Schema.Field pageCoords = imgSchema.getField("pageCoords");
        Schema pageCoordsSchema = pageCoords.schema();
        GenericRecord coords = new GenericData.Record(pageCoordsSchema);
        coords.put("x", 1.0F);
        coords.put("y", 1.0F);
        
        img.put("isImageClick", true);
        img.put("thumbnailId", videoId);
        img.put("pageCoords", coords);
        img.put("windowCoords", coords);
        trackerEvent.put("eventData", img); 
        
        // encode
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

        // Test
        serializer.setEvent(event);
        
        // Test
        List<PutRequest> puts = serializer.getActions();
        // should be zero size
        assertTrue(puts.size() == 0); 
        
        // Test 
        long timestamp = 1416612478000L;
        Date date = new Date(timestamp);
        DateFormat format = new SimpleDateFormat("YYYY-MM-dd'T'HH");
        byte[] formattedTimestamp = format.format(date).getBytes();
        String eventTimestamp = new String(formattedTimestamp);
        
        List<AtomicIncrementRequest> incs = serializer.getIncrements();
        
        assertTrue(incs.size() == 2);
        
        AtomicIncrementRequest req = incs.get(0);
        String key = videoId + "_" + eventTimestamp;
        assertTrue(Arrays.equals(req.key(), key.getBytes()));
        assertTrue(req.getAmount() == 1);
        
        req = incs.get(1);
        key = eventTimestamp + "_" + videoId;
        assertTrue(Arrays.equals(req.key(), key.getBytes()));
        assertTrue(req.getAmount() == 1);
    }

    @Test
    public void test_ImageLoad() throws Exception { 
        
        String videoId = "test_ImageLoad";
    
        Schema writerSchema = new TrackerEvent().getSchema();
        GenericData.Record trackerEvent = new GenericData.Record(writerSchema);
        
        dummyFill(trackerEvent, writerSchema);
        
        GenericData.EnumSymbol eventType = new GenericData.EnumSymbol(writerSchema, "IMAGE_LOAD");
        trackerEvent.put("eventType", eventType);
        
        Schema.Field eventData = writerSchema.getField("eventData");
        Schema eventDataSchema = eventData.schema();
        int i = eventDataSchema.getIndexNamed("com.neon.Tracker.ImageLoad");
        GenericRecord img = new GenericData.Record(eventDataSchema.getTypes().get(i));
        img.put("thumbnailId", new Utf8(videoId));
        
        img.put("height", 1);
        img.put("width", 1);
        
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

        // Test
        serializer.setEvent(event);
        
        // Test
        List<PutRequest> puts = serializer.getActions();
        // should be zero size
        assertTrue(puts.size() == 0); 
        
        // Test 
        long timestamp = 1416612478000L;
        Date date = new Date(timestamp);
        DateFormat format = new SimpleDateFormat("YYYY-MM-dd'T'HH");
        byte[] formattedTimestamp = format.format(date).getBytes();
        String eventTimestamp = new String(formattedTimestamp);
        
        List<AtomicIncrementRequest> incs = serializer.getIncrements();
        
        assertTrue(incs.size() == 2);
        
        AtomicIncrementRequest req = incs.get(0);
        String key = videoId + "_" + eventTimestamp;
        assertTrue(Arrays.equals(req.key(), key.getBytes()));
        assertTrue(req.getAmount() == 1);
        
        req = incs.get(1);
        key = eventTimestamp + "_" + videoId;
        assertTrue(Arrays.equals(req.key(), key.getBytes()));
        assertTrue(req.getAmount() == 1);
    }

    @Test
    public void test_ImagesLoaded() throws Exception { 
        
        String videoId_1 = "test_ImagesLoad_1";
        String videoId_2 = "test_ImagesLoad_2";
    
        Schema writerSchema = new TrackerEvent().getSchema();
        GenericData.Record trackerEvent = new GenericData.Record(writerSchema);
        
        dummyFill(trackerEvent, writerSchema);
        
        GenericData.EnumSymbol eventType = new GenericData.EnumSymbol(writerSchema, "IMAGES_LOAD");
        trackerEvent.put("eventType", eventType);
        
        Schema.Field eventData = writerSchema.getField("eventData");
        Schema eventDataSchema = eventData.schema();
        int i = eventDataSchema.getIndexNamed("com.neon.Tracker.ImagesLoaded");
        Schema imagesLoadedSchema = eventDataSchema.getTypes().get(i);
        GenericRecord imagesLoad = new GenericData.Record(imagesLoadedSchema);
        imagesLoad.put("isImagesLoaded", true);
        
        // make arrays of imageLoad
        Schema.Field imagesField = imagesLoadedSchema.getField("images");
        Schema imagesSchema = imagesField.schema();
        GenericArray<GenericRecord> images = new GenericArray<GenericRecord>(imagesSchema);
        
        // make a couple of imageLoad 
        i = eventDataSchema.getIndexNamed("com.neon.Tracker.ImageLoad");
        Schema imageLoadSchema = eventDataSchema.getTypes().get(i);
        
        GenericRecord imgLoad_1 = new GenericData.Record(imageLoadSchema);
        imgLoad_1.put("thumbnailId", new Utf8(videoId_1));
        imgLoad_1.put("height", 1);
        imgLoad_1.put("width", 1);
        
        GenericRecord imgLoad_2 = new GenericData.Record(imageLoadSchema);
        imgLoad_2.put("thumbnailId", new Utf8(videoId_2));
        imgLoad_2.put("height", 1);
        imgLoad_2.put("width", 1);
        
        // assemble all parts together
        images.add(0, imgLoad_1);
        
        //
        imagesLoad.put("images", images);
        
        //
        trackerEvent.put("eventData", imagesLoad); 
        
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

        // Test
        serializer.setEvent(event);
        
        // Test
        List<PutRequest> puts = serializer.getActions();
        // should be zero size
        assertTrue(puts.size() == 0); 
        
        // Test 
        long timestamp = 1416612478000L;
        Date date = new Date(timestamp);
        DateFormat format = new SimpleDateFormat("YYYY-MM-dd'T'HH");
        byte[] formattedTimestamp = format.format(date).getBytes();
        String eventTimestamp = new String(formattedTimestamp);
        
        List<AtomicIncrementRequest> incs = serializer.getIncrements();
       
        assertTrue(incs.size() == 2);
        
        AtomicIncrementRequest req = incs.get(0);
        String key = videoId_1 + "_" + eventTimestamp;
        assertTrue(Arrays.equals(req.key(), key.getBytes()));
        assertTrue(req.getAmount() == 1);
        
        req = incs.get(1);
        key = eventTimestamp + "_" + videoId_1;
        assertTrue(Arrays.equals(req.key(), key.getBytes()));
        assertTrue(req.getAmount() == 1);
        
        req = incs.get(0);
        key = videoId_2 + "_" + eventTimestamp;
        assertTrue(Arrays.equals(req.key(), key.getBytes()));
        assertTrue(req.getAmount() == 1);
        
        req = incs.get(1);
        key = eventTimestamp + "_" + videoId_2;
        assertTrue(Arrays.equals(req.key(), key.getBytes()));
        assertTrue(req.getAmount() == 1);
    }


    /*
    *   Utils
    */
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
    
    private static void dummyFill(GenericData.Record trackerEvent, Schema writerSchema) {
        trackerEvent.put("pageId", new Utf8("pageId_dummy"));
        trackerEvent.put("trackerAccountId", new Utf8("trackerAccountId_dummy"));

        GenericData.EnumSymbol trackerType = new GenericData.EnumSymbol(writerSchema, "IGN");
        trackerEvent.put("trackerType", trackerType);
        
        trackerEvent.put ("pageURL", new Utf8("pageUrl_dummy"));
        trackerEvent.put ("refURL", new Utf8("refUrl_dummy"));
        
        trackerEvent.put("serverTime", 1000L);
        trackerEvent.put("clientTime", 1000L);
        
        trackerEvent.put("clientIP", new Utf8("clientIp_dummy"));
        trackerEvent.put ("neonUserId", new Utf8("neonUserId_dummy"));
        trackerEvent.put("userAgent", new Utf8("userAgent_dummy"));
        
        GenericData.Record agentInfo = new GenericData.Record(writerSchema);
        trackerEvent.put("agentInfo", null);
    
        Schema.Field geoDataField = writerSchema.getField("ipGeoData");
        GenericRecord geoData = new GenericData.Record(geoDataField.schema());
        geoData.put("country", new Utf8("usa"));
        trackerEvent.put("ipGeoData", geoData); 
    }
    
    public static void main(String[] args) {
        System.out.println("\n\nTest Starting"); 

        try {
            NeonGenericSerializerTest serializer = new NeonGenericSerializerTest();
            
            serializer.test_ImageVisible_Base();
            serializer.test_ImagesVisible();
            serializer.test_ImageClick();
            serializer.test_ImageLoad();
            serializer.test_ImagesLoaded();
           
            // testing changes in schemas
            serializer = new NeonGenericSerializerTest();
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






