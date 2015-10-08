/**
 * 
 */
package com.neon.flume;

import org.apache.flume.instrumentation.MonitoredCounterGroup;

/**
 * @author mdesnoyer
 * 
 */
public class NeonAvroSerializerCounter extends MonitoredCounterGroup {
  public static final String COUNTER_VALID_EVENTS =
      "sink.serializer.events.valid";
  public static final String COUNTER_INVALID_EVENTS =
      "sink.serializer.events.invalid";
  public static final String COUNTER_EVENT_READ_ERRORS =
      "sink.serializer.events.read_errors";
  public static final String COUNTER_EVENT_MISSING_TID =
      "sink.serializer.events.missing_thumbnailid";
  public static final String COUNTER_EVENT_MISSING_TIME =
      "sink.serializer.events.missing_timestamp";
  public static final String COUNTER_SCHEMA_CONNECTION_ERRORS =
      "sink.serializer.schema.connection_errors";
  public static final String COUNTER_SCHEMA_INVALID_ERRORS =
      "sink.serializer.schema.invlid_errors";
  public static final String COUNTER_SCHEMA_FOUND =
      "sink.serializer.schema.found";
  public static final String COUNTER_ERROR_SET_EVENT =
      "sink.serializer.unexpected_error.set_event";
  public static final String COUNTER_ERROR_GET_INCREMENTS =
      "sink.serializer.unexpected_error.get_increments";

  private static final String[] ATTRIBUTES = { COUNTER_VALID_EVENTS,
      COUNTER_INVALID_EVENTS, COUNTER_SCHEMA_CONNECTION_ERRORS,
      COUNTER_EVENT_READ_ERRORS, COUNTER_EVENT_MISSING_TID,
      COUNTER_EVENT_MISSING_TIME, COUNTER_SCHEMA_INVALID_ERRORS,
      COUNTER_SCHEMA_FOUND, COUNTER_ERROR_SET_EVENT,
      COUNTER_ERROR_GET_INCREMENTS };

  public NeonAvroSerializerCounter(String name) {
    super(MonitoredCounterGroup.Type.SERIALIZER, name, ATTRIBUTES);
  }
  
  /*
   * Make the increment public. You must use one of the COUNTER variables in from this class.
   * @see org.apache.flume.instrumentation.MonitoredCounterGroup#increment(java.lang.String)
   */
  public long increment(String counter) {
    return super.increment(counter);
  }
}
