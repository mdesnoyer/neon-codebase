package com.neon.flume;


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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;


class NeonSerializerTest {   
    


    public static void fillWithDummies(TrackerEvent event) {

        event.setPageId("pageId_dummy");
        event.setTrackerAccountId("trackerAccountId_dummy");
        event.setTrackerType(com.neon.flume.TrackerType.IGN);
        event.setPageURL ("pageUrl_dummy");
        event.setRefURL ("refUrl_dummy");
        event.setServerTime (new java.lang.Long(1000));
        event.setClientTime (new java.lang.Long(1000) );
        event.setClientIP("clientIp_dummy");
        event.setNeonUserId ("neonUserId_dummy");
        event.setUserAgent("userAgent_dummy");
        event.setAgentInfo(new com.neon.flume.AgentInfo());
        event.setIpGeoData(new com.neon.flume.GeoData()); 
    }


    public static void test_ImageVisible() throws Exception { 

        //Schema schema = new Schema.Parser().parse(new File("schema.avsc"));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        //Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        //GenericDatumWriter writer = new GenericDatumWriter(schema);
        DatumWriter<TrackerEvent> writer = new SpecificDatumWriter<TrackerEvent>(TrackerEvent.class);
        DataFileWriter<TrackerEvent> streamWriter = new DataFileWriter<TrackerEvent>(writer);
        
        //GenericRecord trackerEvent = new GenericData.Record(schema);
        //trackerEvent.put("name", "TrackerEvent");
        //trackerEvent.put("namespace", "com.neon.Tracker");
        
        TrackerEvent trackerEvent = new TrackerEvent(); 
        fillWithDummies(trackerEvent);

        ImageVisible i = new ImageVisible();
        i.setThumbnailId("t1");
        trackerEvent.setEventType(com.neon.flume.EventType.IMAGE_VISIBLE);
        trackerEvent.setEventData(i);

        System.out.println(trackerEvent);

        //writer.write(trackerEvent, encoder);
        //encoder.flush();

        streamWriter.create(trackerEvent.getSchema(), outputStream);
        streamWriter.append(trackerEvent);
        streamWriter.close();
        byte[] encodedEvent = outputStream.toByteArray();

        // make avro container headers
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("flume.avro.schema.url"," https://s3.amazonaws.com/neon-avro-schema/3325be34d95af2ca7d2db2b327e93408.avsc" );
        //headers.put("timestamp", "2014-11-05T13:15:30Z");
        headers.put("timestamp", "1416612478");

        Event event = EventBuilder.withBody(encodedEvent, headers);
        NeonSerializer serializer = new NeonSerializer();

        String table = "table";
        String columnFamily = "columFamily";
        serializer.initialize(table.getBytes(), columnFamily.getBytes());

        /*
         *  Test 
         */
        serializer.setEvent(event);

        /*
         * Test
         */
        List<PutRequest> puts = serializer.getActions();

        /*
         * Test
         */
        List<AtomicIncrementRequest> incs =serializer.getIncrements();
    }

    
    public static void main(String[] args) {
        System.out.println("\n\nTest Starting"); 


        try {

            test_ImageVisible();
            System.out.println("\n\nTest sucessful");
        }
        catch(Exception e) {
            System.out.println("Test failure: exception: " + e.toString());
        }
    
    }
}






