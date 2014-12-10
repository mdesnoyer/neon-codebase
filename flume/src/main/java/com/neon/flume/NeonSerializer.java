package com.neon.flume;

import  com.neon.Tracker.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.regex.*;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.util.Date;

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
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.file.SeekableByteArrayInput;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.SimpleHbaseEventSerializer.KeyType;
import org.apache.flume.sink.hbase.AsyncHbaseEventSerializer;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;

public class NeonSerializer implements AsyncHbaseEventSerializer 
{
    // to hold hbase operations 
    private final List<PutRequest> actions = new ArrayList<PutRequest>();
    private final List<AtomicIncrementRequest> increments = new ArrayList<AtomicIncrementRequest>();

    // hbase tables
    private static final byte[] THUMBNAIL_FIRST_TABLE = "THUMBNAIL_TIMESTAMP_EVENTS".getBytes();
    private static final byte[] TIMESTAMP_FIRST_TABLE = "TIMESTAMP_THUMBNAIL_EVENTS".getBytes();

    // column family to store counters, one for each event type
    private static final byte[] COLUMN_FAMILY = "THUMBNAIL_EVENTS_TYPES".getBytes();
    
    // column for the counter of IMAGE_VISIBLE and IMAGES_VISIBLE events
    private static final byte IMAGE_VISIBLE_COLUMN_NAME[] = "IMAGE_VISIBLE".getBytes();
    
    // column for the counter of IMAGE_LOAD and IMAGES_LOADED events
    private static final byte IMAGE_LOAD_COLUMN_NAME[] = "IMAGE_LOAD".getBytes();
    
    private byte IMAGE_CLICK_COLUMN_NAME[] = "IMAGE_CLICK".getBytes();
        
    // event-based  
    private String eventTimestamp = null;
    private TrackerEvent trackerEvent = null;
    private String rowKey = null;

    @Override
    public void initialize(byte[] table, byte[] cf) 
    {
        trackerEvent = null;
        eventTimestamp = null;
        rowKey = null;
    }

    /*
    *  Sink calls this method first on any event.  This is where we decode the 
    *  event and keep a ref to it.  The sink will call us next with getActions() 
    *  and getIncrements().  A failure to decode sets a null object, so that we 
    *  simply return empty results in these calls.
    */
    @Override
    public void setEvent(Event event) 
    {
      trackerEvent = null;
      
      try {
          // obtain the timestamp of event
          String t = event.getHeaders().get("timestamp");
          
          // currently this is received in milliseconds
          long timestamp = Long.valueOf(t).longValue();
          
          // convert to readable format
          Date date = new Date(timestamp);
          DateFormat format = new SimpleDateFormat("YYYY-MM-dd'T'HH");
          byte[] formattedTimestamp = format.format(date).getBytes();
          eventTimestamp = new String(formattedTimestamp);

           // decode the tracker event
           DatumReader<TrackerEvent> datumReader = new SpecificDatumReader<TrackerEvent>(TrackerEvent.class);
           BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(event.getBody(), null);
           trackerEvent = datumReader.read(null, binaryDecoder);
        }
        catch(IOException e) {
            trackerEvent = null;
            throw new FlumeException(e.toString());
        }
        catch(Exception e) {
            trackerEvent = null;
            throw new FlumeException(e.toString());
        }
    }
 
    /*
    *  Sink calls this method second to get any row creation operations needed.
    *  We have no row creations to do in this application so we return an empty
    *  list.
    */
    @Override
    public List<PutRequest> getActions() 
    {
        // no-ops here
        actions.clear();
        return actions;
    } 
 
    /*
    *  Sink calls this method third to get any increment operations needed.
    *  Note that the sink will coalesce these increment ops for performance.
    */
    @Override
    public List<AtomicIncrementRequest> getIncrements() 
    {
        increments.clear();

        // if this event was dropped previously, do nothing 
        if(trackerEvent == null) {
            return increments;
        }

        // extract and process each thumbnails in this event
        switch(trackerEvent.getEventType())  {

            case IMAGE_VISIBLE:
                ImageVisible imgVis = (ImageVisible) trackerEvent.getEventData();
                handleIncrement(imgVis.getThumbnailId().toString(), IMAGE_VISIBLE_COLUMN_NAME);
                break;

            case IMAGES_VISIBLE:
                ImagesVisible imgsVis = (ImagesVisible) trackerEvent.getEventData();
                for(CharSequence tid: imgsVis.thumbnailIds)
                    handleIncrement(tid.toString(), IMAGE_VISIBLE_COLUMN_NAME);
                break;

            case IMAGE_CLICK:
                ImageClick imgClk = (ImageClick) trackerEvent.getEventData();
                handleIncrement(imgClk.getThumbnailId().toString(), IMAGE_CLICK_COLUMN_NAME);
                break;
                
            case IMAGE_LOAD:
                ImageLoad imgLd = (ImageLoad) trackerEvent.getEventData();
                handleIncrement(imgLd.getThumbnailId().toString(), IMAGE_LOAD_COLUMN_NAME);
                break;

            case IMAGES_LOADED:
                ImagesLoaded imgLded = (ImagesLoaded) trackerEvent.getEventData();
                for(CharSequence tid: imgLded.thumbnailIds)
                    handleIncrement(tid.toString(), IMAGE_LOAD_COLUMN_NAME);
                break;
                
            // event types we're not insterested in result in no-ops
            default:
                return increments;
        }
        return increments;
    }

    // this method depends on hbase to create a row automatically on first increment request
    private void handleIncrement(String tid, byte[] columnName) 
    {
        // increment counter in table which begins with thumbnail first composite key
        String key = tid  + "_" + eventTimestamp;
        increments.add(new AtomicIncrementRequest(THUMBNAIL_FIRST_TABLE, key.getBytes(), COLUMN_FAMILY, columnName));

        // increment counter in table which begins with timestamp first composite key
        key = eventTimestamp + "_" + tid;
        increments.add(new AtomicIncrementRequest(TIMESTAMP_FIRST_TABLE, key.getBytes(), COLUMN_FAMILY,  columnName));
    }

    @Override
    public void cleanUp() 
    {
        trackerEvent = null;
        eventTimestamp = null;
        trackerEvent = null;
    }
 
    @Override
    public void configure(Context context) {}
 
    @Override
    public void configure(ComponentConfiguration conf) {}

}






