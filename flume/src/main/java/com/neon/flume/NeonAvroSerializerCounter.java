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
  public static final String COUNTER_VALID_EVENTS = "sink.serializer.events.valid";
  public static final String COUNTER_INVALID_EVENTS = "sink.serializer.events.invalid";
  public static final String COUNTER_EVENT_WRITE_ERRORS = "sink.serializer.events.write_errors";
  public static final String COUNTER_SCHEMA_CONNECTION_ERRORS = "sink.serializer.schema.connection_errors";
  public static final String COUNTER_URL_NOT_FOUND = "sink.serializer.schema.rul_not_found";

  private static final String[] ATTRIBUTES = { COUNTER_VALID_EVENTS, COUNTER_INVALID_EVENTS,
      COUNTER_SCHEMA_CONNECTION_ERRORS, COUNTER_EVENT_WRITE_ERRORS, COUNTER_URL_NOT_FOUND };

  public NeonAvroSerializerCounter(String name) {
    super(MonitoredCounterGroup.Type.SERIALIZER, name, ATTRIBUTES);
  }

  /*
   * Make the increment public. You must use one of the COUNTER variables in
   * from this class.
   * 
   * @see
   * org.apache.flume.instrumentation.MonitoredCounterGroup#increment(java.lang.
   * String)
   */
  public long increment(String counter) {
    return super.increment(counter);
  }
}
