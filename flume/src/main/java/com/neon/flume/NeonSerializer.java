package com.neon.flume;

import  com.neon.Tracker.*;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.*;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
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
import org.apache.avro.file.SeekableByteArrayInput;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.SimpleHbaseEventSerializer.KeyType;
import org.apache.flume.sink.hbase.AsyncHbaseEventSerializer;
 
//import com.google.common.base.Charsets;



public class NeonSerializer implements AsyncHbaseEventSerializer 
{
    private final List<PutRequest> actions = new ArrayList<PutRequest>();
    private final List<AtomicIncrementRequest> increments = new ArrayList<AtomicIncrementRequest>();

    // hbase tables to store all events
    private byte[] thumbnailFirstTable = "THUMBNAIL_TIMESTAMP_EVENTS".getBytes();
    private byte[] timestampFirstTable = "TIMESTAMP_THUMBNAIL_EVENTS".getBytes();

    // column family to store counters, one for each event type
    private byte[] columnFamily = "THUMBNAIL_EVENTS_TYPES".getBytes();
    byte imageVisibleColumnName[] = "IMAGE_VISIBLE".getBytes();
    byte imageLoadColumnName[] = "IMAGE_LOAD".getBytes();
    byte imageClickColumnName[] = "IMAGE_CLICK".getBytes();
        

    // event-based  
    private String eventTimestamp;
    private TrackerEvent trackerEvent;
    private String rowKey;

    @Override
    public void initialize(byte[] table, byte[] cf) 
    {
        trackerEvent = null;
        eventTimestamp = null;
        rowKey = null;
    }

    @Override
    public void setEvent(Event event) 
    {
        trackerEvent = null;
        
        try {

            // obtain the timestamps of event
            String t = event.getHeaders().get("timestamp");
            long timestamp = Long.valueOf(t).longValue();

            // remove minutes and seconds precision
            timestamp /= 3600;
            eventTimestamp = Long.toString(timestamp); 

            // obtain the tracker event
            SeekableByteArrayInput input = new SeekableByteArrayInput(event.getBody());  
            DatumReader<TrackerEvent> datumReader = new SpecificDatumReader<TrackerEvent>(TrackerEvent.class);
            DataFileReader<TrackerEvent> dataFileReader = new DataFileReader<TrackerEvent>(input, datumReader);

            // if no tracker event, drop event
            if(dataFileReader.hasNext() == false) {
                trackerEvent = null;
                return;
            }

            // save tracker event for next calls to getActions(), getIncrements()
            trackerEvent = dataFileReader.next(trackerEvent);
            System.out.println(trackerEvent);    
        }
        catch(IOException e) {
            trackerEvent = null;
            return;
        }
        catch(Exception e) {
            trackerEvent = null;
            return;
        }
    }
 
    @Override
    public List<PutRequest> getActions() 
    {
            // no row creation
            return null;
    } 
 
    @Override
    public List<AtomicIncrementRequest> getIncrements() 
    {
        // if this event was dropped in previously do nothing 
        if(trackerEvent == null)
            return null;

        increments.clear();
        
        //Increment the number of events by one implicitly
        switch(trackerEvent.getEventType())  {
                     
            case IMAGE_VISIBLE: 
                ImageVisible imgVis = (ImageVisible) trackerEvent.getEventData();
                handleIncrement(imgVis.getThumbnailId().toString(), imageVisibleColumnName);
                break;
       
            case IMAGE_CLICK: 
                ImageClick imgClk = (ImageClick) trackerEvent.getEventData();
                handleIncrement(imgClk.getThumbnailId().toString(), imageClickColumnName);
                break;

            default:
                return null;
        }
        
        return increments; 
    }

    // this method depends on hbase to create a row automatically on first increment request
    private void handleIncrement(String tid, byte[] columnName) 
    {
        // increment counter in table with thumbnail first composite key
        String key = tid  + "_" + eventTimestamp;
        increments.add(new AtomicIncrementRequest(thumbnailFirstTable, key.getBytes(), columnFamily, columnName));        
 
        // increment counter in table with timestamp first composite key
        key = eventTimestamp + "_" + tid;
        increments.add(new AtomicIncrementRequest(timestampFirstTable, key.getBytes(), columnFamily,  columnName));
    }



    @Override
    public void cleanUp() 
    {
        trackerEvent = null;
        eventTimestamp = null;
        trackerEvent = null;
    }
 
    @Override
    public void configure(Context context) 
    {
        // config hard-coded
    }
 
    @Override
    public void configure(ComponentConfiguration conf) {}

}






