package com.neon.flume;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.AsyncHbaseEventSerializer;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;

import com.neon.Tracker.*;

public class NeonGenericSerializer implements AsyncHbaseEventSerializer {
  final static Logger logger = Logger.getLogger(NeonGenericSerializer.class);

  // to hold hbase operations
  private final List<PutRequest> actions = new ArrayList<PutRequest>();
  private final List<AtomicIncrementRequest> increments =
      new ArrayList<AtomicIncrementRequest>();

  // hbase tables
  private static final byte[] THUMBNAIL_FIRST_TABLE =
      "THUMBNAIL_TIMESTAMP_EVENT_COUNTS".getBytes();
  private static final byte[] TIMESTAMP_FIRST_TABLE =
      "TIMESTAMP_THUMBNAIL_EVENT_COUNTS".getBytes();

  // column family to store counters, one for each event type
  private static final byte[] COLUMN_FAMILY = "evts".getBytes();

  // column for the counter of IMAGE_VISIBLE and IMAGES_VISIBLE events
  private static final byte[] IMAGE_VISIBLE_COLUMN_NAME = "iv".getBytes();

  // column for the counter of IMAGE_LOAD and IMAGES_LOADED events
  private static final byte[] IMAGE_LOAD_COLUMN_NAME = "il".getBytes();

  // column for the counter of IMAGE_CLICK events
  private static final byte[] IMAGE_CLICK_COLUMN_NAME = "ic".getBytes();

  // tracker event header of the schema in use
  public static final String AVRO_SCHEMA_URL_HEADER = "flume.avro.schema.url";

  // A cache of event readers for different schemas. Maps the url to the reader object
  private Map<String, GenericDatumReader<TrackerEvent>> readerCache = new HashMap<String, GenericDatumReader<TrackerEvent>>();
  
  // State counters
  NeonHBaseSerializerCounter counters = null;

  // event-based
  private String eventTimestamp = null;
  private GenericRecord trackerEvent = null;
  private GenericDatumReader<TrackerEvent> eventReader = null;

  // Object used to open urls so that it can be mocked
  private URLOpener urlOpener;

  // Configuration parameters

  /*
   * Constructor that should only be used for unittesting
   * 
   * @param urlOpener
   */
  public NeonGenericSerializer(URLOpener urlOpener) {
    this.urlOpener = urlOpener;
  }

  public NeonGenericSerializer() {
    this.urlOpener = new URLOpener();
  }

  @Override
  public void initialize(byte[] table, byte[] cf) {
    // The table and column family configuration are ignored
  }

  /**
   * Sink calls this method first on any event. This is where we decode the
   * event and keep a ref to it. The sink will call us next with getActions()
   * and getIncrements(). A failure to decode sets a null object, so that we
   * simply return empty results in these calls.
   */
  @Override
  public void setEvent(Event event) {
    trackerEvent = null;
    try {
      // Get the schema information
      String schemaURL = "";
      try {
        // fetch needed schema for decoding either from cache or S3
        schemaURL = event.getHeaders().get(AVRO_SCHEMA_URL_HEADER);

        // see if we have the schema already
        eventReader = readerCache.get(schemaURL);

        if (eventReader == null) {
          // try getting schema from S3 then
          Schema schema = loadFromUrl(schemaURL);

          if (schema == null) {
            // unable to fetch needed schema, drop event
            logger.error("unable to fetch schema, event dropped. url "
                + schemaURL);
            counters.increment(NeonHBaseSerializerCounter.COUNTER_SCHEMA_CONNECTION_ERRORS);
            return;
          }
          
          eventReader = new GenericDatumReader<TrackerEvent>(schema, TrackerEvent.getClassSchema());

          // add to reader cache
          readerCache.put(schemaURL, eventReader);
          logger.info("added new schema to cache: url " + schemaURL);
          counters.increment(NeonHBaseSerializerCounter.COUNTER_SCHEMA_FOUND);
        }
      } catch (IOException e) {
        logger.error("Unable to download the schema at " + schemaURL + " : "
            + e.toString());
        counters.increment(NeonHBaseSerializerCounter.COUNTER_SCHEMA_CONNECTION_ERRORS);
        return;
      } catch (AvroRuntimeException e) {
        logger.error("Error resolving avro schema " + e.toString());
        counters.increment(NeonHBaseSerializerCounter.COUNTER_SCHEMA_INVALID_ERRORS);
        counters.increment(NeonHBaseSerializerCounter.COUNTER_INVALID_EVENTS);
        return;
      }

      // obtain the timestamp of event
      String t = event.getHeaders().get("timestamp");

      if (t == null || t.equals("")) {
        logger.error("Unable to obtain timestamp header, event dropped");
        counters.increment(NeonHBaseSerializerCounter.COUNTER_EVENT_MISSING_TIME);
        counters.increment(NeonHBaseSerializerCounter.COUNTER_INVALID_EVENTS);
        return;
      }

      // currently this is received in milliseconds
      long timestamp = Long.valueOf(t).longValue();

      // convert to readable format
      Date date = new Date(timestamp);
      DateFormat format = new SimpleDateFormat("YYYY-MM-dd'T'HH");
      format.setTimeZone(TimeZone.getTimeZone("UTC"));
      byte[] formattedTimestamp = format.format(date).getBytes();
      eventTimestamp = new String(formattedTimestamp);

      // decode the tracker event
      BinaryDecoder binaryDecoder =
          DecoderFactory.get().binaryDecoder(event.getBody(), null);
      
      try {
        trackerEvent = eventReader.read(null, binaryDecoder);
      } catch (IOException e) {
        logger.error("Error reading avro event " + e.toString());
        counters.increment(NeonHBaseSerializerCounter.COUNTER_EVENT_READ_ERRORS);
        counters.increment(NeonHBaseSerializerCounter.COUNTER_INVALID_EVENTS);
        return;
      } catch (AvroRuntimeException e) {
        logger.error("Error parsing avro event " + e.toString());
        counters.increment(NeonHBaseSerializerCounter.COUNTER_EVENT_READ_ERRORS);
        counters.increment(NeonHBaseSerializerCounter.COUNTER_INVALID_EVENTS);
        return;
      }

      if (trackerEvent == null) {
        logger.error("unable to parse event");
        counters.increment(NeonHBaseSerializerCounter.COUNTER_EVENT_READ_ERRORS);
        counters.increment(NeonHBaseSerializerCounter.COUNTER_INVALID_EVENTS);
        return;
      }

    } catch (Exception e) {
      logger.error("Unexpected error when setting the event ", e);
      counters.increment(NeonHBaseSerializerCounter.COUNTER_ERROR_SET_EVENT);
      counters.increment(NeonHBaseSerializerCounter.COUNTER_INVALID_EVENTS);
      return;
    }
    counters.increment(NeonHBaseSerializerCounter.COUNTER_VALID_EVENTS);
  }

  /**
   * Sink calls this method second to get any row creation operations needed. We
   * have no row creations to do in this application so we return an empty list.
   */
  @Override
  public List<PutRequest> getActions() {
    // always no-ops here
    actions.clear();
    return actions;
  }

  /**
   * Sink calls this method third to get any increment operations needed. Note
   * that the sink will coalesce these increment ops for performance.
   */
  @Override
  public List<AtomicIncrementRequest> getIncrements() {
    increments.clear();

    // if this event was dropped previously, do nothing
    if (trackerEvent == null) {
      return increments;
    }

    try {
      // extract event type and process it as generically as possible
      GenericEnumSymbol eventType =
          (GenericEnumSymbol) trackerEvent.get("eventType");
      String type = eventType.toString();
      GenericRecord eventData = (GenericRecord) trackerEvent.get("eventData");

      if (type.equals("IMAGE_VISIBLE")) {
        handleIncrement(eventData.get("thumbnailId").toString(),
            IMAGE_VISIBLE_COLUMN_NAME);
      }

      else if (type.equals("IMAGES_VISIBLE")) {
        // array of string type
        GenericArray thumbs = (GenericArray) eventData.get("thumbnailIds");
        for (Object tid : thumbs)
          handleIncrement(tid.toString(), IMAGE_VISIBLE_COLUMN_NAME);
      }

      else if (type.equals("IMAGE_CLICK")) {
        handleIncrement(eventData.get("thumbnailId").toString(),
            IMAGE_CLICK_COLUMN_NAME);
      }

      else if (type.equals("IMAGE_LOAD")) {
        handleIncrement(eventData.get("thumbnailId").toString(),
            IMAGE_LOAD_COLUMN_NAME);
      }

      else if (type.equals("IMAGES_LOADED")) {
        GenericArray<GenericRecord> images =
            (GenericArray<GenericRecord>) eventData.get("images");
        for (GenericRecord img : images) {
          String tid = img.get("thumbnailId").toString();
          handleIncrement(tid, IMAGE_LOAD_COLUMN_NAME);
        }
      }
    } catch (Exception e) {
      trackerEvent = null;
      increments.clear();
      logger.error("Unexpected error while creating increments, event dropped.", e);
      counters.increment(NeonHBaseSerializerCounter.COUNTER_ERROR_GET_INCREMENTS);
    }

    return increments;
  }

  private void handleIncrement(String tid, byte[] columnName) {
    // discard if tid malformed
    if (isMalformedThumbnailId(tid, columnName))
      return;

    // increment counter in table which begins with thumbnail first composite
    // key
    String key = tid + "_" + eventTimestamp;
    increments.add(new AtomicIncrementRequest(THUMBNAIL_FIRST_TABLE, key
        .getBytes(), COLUMN_FAMILY, columnName));

    // increment counter in table which begins with timestamp first composite
    // key
    key = eventTimestamp + "_" + tid;
    increments.add(new AtomicIncrementRequest(TIMESTAMP_FIRST_TABLE, key
        .getBytes(), COLUMN_FAMILY, columnName));
  }

  private Schema loadFromUrl(String schemaUrl) throws IOException {
    Schema.Parser parser = new Schema.Parser();
    InputStream is = null;
    try {
      is = urlOpener.open(schemaUrl);
      return parser.parse(is);
    } finally {
      if (is != null) {
        is.close();
      }
    }
  }

  private boolean isMalformedThumbnailId(String tid, byte[] columnName) {

    if (tid == null) {
      logger.error("thumbnail id is null for column family "
          + columnName.toString());
      counters.increment(NeonHBaseSerializerCounter.COUNTER_EVENT_MISSING_TID);
      counters.increment(NeonHBaseSerializerCounter.COUNTER_INVALID_EVENTS);
      return true;
    }

    if (tid.equals("")) {
      logger.error("thumbnail id is empty string for column family "
          + columnName.toString());
      counters.increment(NeonHBaseSerializerCounter.COUNTER_EVENT_MISSING_TID);
      counters.increment(NeonHBaseSerializerCounter.COUNTER_INVALID_EVENTS);
      return true;
    }

    // we may add more checks in the future

    return false;
  }

  @Override
  public void cleanUp() {
    trackerEvent = null;
    eventTimestamp = null;
    trackerEvent = null;
    if (counters != null) {
      counters.stop();
    }
  }

  @Override
  public void configure(Context context) {
    if (counters == null) {
      counters = new NeonHBaseSerializerCounter("hbase_serializer");
      counters.start();
    }
  }

  @Override
  public void configure(ComponentConfiguration conf) {
  }

  /**
   * Class used to open a url. This is needed for mocking purposes because URL
   * is final and cannot be mocked.
   * 
   * @author mdesnoyer
   * 
   */
  public class URLOpener {
    public InputStream open(String url) throws IOException {
      return new URL(url).openStream();
    }
  }

}
