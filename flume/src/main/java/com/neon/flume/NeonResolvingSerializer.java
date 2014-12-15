package com.neon.flume;

import  com.neon.Tracker.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.regex.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.file.SeekableByteArrayInput;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.SimpleHbaseEventSerializer.KeyType;
import org.apache.flume.sink.hbase.AsyncHbaseEventSerializer;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;

public class NeonResolvingSerializer implements AsyncHbaseEventSerializer 
{

    final static Logger logger = Logger.getLogger(NeonGenericSerializer.class);

    // to hold hbase operations 
    private final List<PutRequest> actions = new ArrayList<PutRequest>();
    private final List<AtomicIncrementRequest> increments = new ArrayList<AtomicIncrementRequest>();

    // hbase tables
    private static final byte[] THUMBNAIL_FIRST_TABLE = "THUMBNAIL_TIMESTAMP_EVENTS".getBytes();
    private static final byte[] TIMESTAMP_FIRST_TABLE = "TIMESTAMP_THUMBNAIL_EVENTS".getBytes();

    // column family to store counters, one for each event type
    private static final byte[] COLUMN_FAMILY = "THUMBNAIL_EVENTS_TYPES".getBytes();
    
    // column for the counter of IMAGE_VISIBLE and IMAGES_VISIBLE events
    private static final byte[] IMAGE_VISIBLE_COLUMN_NAME = "IMAGE_VISIBLE".getBytes();
    
    // column for the counter of IMAGE_LOAD and IMAGES_LOADED events
    private static final byte[] IMAGE_LOAD_COLUMN_NAME = "IMAGE_LOAD".getBytes();
    
    // column for the counter of IMAGE_CLICK events
    private static final byte[] IMAGE_CLICK_COLUMN_NAME = "IMAGE_CLICK".getBytes();

    public static final String AVRO_SCHEMA_URL_HEADER = "flume.avro.schema.url";

    private Map<String, Schema> schemaCache = new HashMap<String, Schema>();

    private Schema readerSchema = null; // TrackerEvent.getSchema();
    private Object resolver = null;

    // event-based  
    private String eventTimestamp = null;
    private GenericRecord trackerEvent = null;
    private String rowKey = null;
 
    @Override
    public void initialize(byte[] table, byte[] cf) 
    {
        readerSchema = new TrackerEvent().getSchema();
        
        eventTimestamp = null;
        trackerEvent = null;
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
         
          if(t == null || t.equals("")) {
              logger.error("unable to obtain timestamp header, event dropped"); 
              return;
          }

          // currently this is received in milliseconds
          long timestamp = Long.valueOf(t).longValue();
          
          // convert to readable format
          Date date = new Date(timestamp);
          DateFormat format = new SimpleDateFormat("YYYY-MM-dd'T'HH");
          byte[] formattedTimestamp = format.format(date).getBytes();
          eventTimestamp = new String(formattedTimestamp);

          // fetch needed schema for decoding either from cache or S3  
          String url = event.getHeaders().get(AVRO_SCHEMA_URL_HEADER);

          // see if we have the schema already
          Schema writerSchema = schemaCache.get(url);
          
          if (writerSchema == null) {

              // try getting schema from S3 then
              writerSchema = loadFromUrl(url);
          
              if(writerSchema == null) {
                  // unable to fetch needed schema, drop event
                  logger.error("unable to fetch writer schema, event dropped. url " + url);
                  return;
              }

              if(logger.isInfoEnabled())
                  logger.info("added new writer schema to cache: url " + url);

              // add to schema cache      
              schemaCache.put(url, writerSchema);
          }

          // decode the tracker event
          //resolver = ResolvingDecoder.resolve(writerSchema, readerSchema);
          ByteArrayInputStream in = new ByteArrayInputStream(event.getBody());
          DecoderFactory factory = new DecoderFactory();
          Decoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(in, null);
          //ResolvingDecoder resolvingDecoder = new ResolvingDecoder(resolver, decoder); 
          ResolvingDecoder resolvingDecoder = DecoderFactory.get().resolvingDecoder(writerSchema, readerSchema, decoder);
          GenericDatumReader<Record> datumReader = new GenericDatumReader<Record>(readerSchema);
          GenericData.Record trackerEvent = new GenericData.Record(readerSchema);
          datumReader.read(trackerEvent, resolvingDecoder);
        }
        catch(IOException e) {
            trackerEvent = null;
            logger.error("unable to parse event due to io: " + e.toString());
        }
        catch(Exception e) {
            trackerEvent = null;
            logger.error("unable to parse event: " +e.toString());
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
        // always no-ops here
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

        try {

            // extract event type and process it as generically as possible
            GenericEnumSymbol eventType = (GenericEnumSymbol) trackerEvent.get("eventType");
            String type = eventType.toString();
            GenericRecord eventData = (GenericRecord) trackerEvent.get("eventData");         
        
            if(type.equals("IMAGE_VISIBLE")) {
                    handleIncrement(eventData.get("thumbnailId").toString(), IMAGE_VISIBLE_COLUMN_NAME);
            }

            else if (type.equals("IMAGES_VISIBLE")) {
                // array of string type
                GenericArray thumbs = (GenericArray) eventData.get("thumbnailIds");
                for(Object tid: thumbs) 
                    handleIncrement(tid.toString(), IMAGE_VISIBLE_COLUMN_NAME);
            }
            
            else if (type.equals("IMAGE_CLICK")) {
                handleIncrement(eventData.get("thumbnailId").toString(), IMAGE_CLICK_COLUMN_NAME); 
            }
            
            else if (type.equals("IMAGE_LOAD")) {
                handleIncrement(eventData.get("thumbnailId").toString(), IMAGE_LOAD_COLUMN_NAME); 
            }
            
            else if (type.equals("IMAGES_LOADED")) {
                GenericArray<GenericRecord> images = (GenericArray<GenericRecord>) eventData.get("images");
                for(GenericRecord img: images) {
                    String tid = img.get("thumbnailId").toString();
                    handleIncrement(tid, IMAGE_LOAD_COLUMN_NAME);
                }
            }
        }
        catch(Exception e) {
            trackerEvent = null;
            increments.clear();
            logger.error("error while extracting thumbnail ids, event dropped.  " + e.toString());      
        }

        return increments;
    }

    private void handleIncrement(String tid, byte[] columnName) 
    {
        // discard if tid malformed
        if(isMalformedThumbnailId(tid, columnName)) 
            return;

        // increment counter in table which begins with thumbnail first composite key
        String key = tid  + "_" + eventTimestamp;
        increments.add(new AtomicIncrementRequest(THUMBNAIL_FIRST_TABLE, key.getBytes(), COLUMN_FAMILY, columnName));

        // increment counter in table which begins with timestamp first composite key
        key = eventTimestamp + "_" + tid;
        increments.add(new AtomicIncrementRequest(TIMESTAMP_FIRST_TABLE, key.getBytes(), COLUMN_FAMILY,  columnName));
    }

    private Schema loadFromUrl(String schemaUrl) throws IOException {
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

    private static boolean isMalformedThumbnailId(String tid, byte[] columnName) {

        if(tid == null) {
            logger.error("thumbnail id is null for column family " + columnName.toString());
            return false;
        }

        if(tid.equals("")) {
            logger.error("thumbnail id is empty string for column family " + columnName.toString());
            return false;
        }
    
        // we may add more checks in the future

        return true;
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






