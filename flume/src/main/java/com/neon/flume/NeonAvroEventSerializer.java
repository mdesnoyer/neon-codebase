/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.neon.flume;

import static org.apache.flume.serialization.AvroEventSerializerConfigurationConstants.COMPRESSION_CODEC;
import static org.apache.flume.serialization.AvroEventSerializerConfigurationConstants.DEFAULT_COMPRESSION_CODEC;
import static org.apache.flume.serialization.AvroEventSerializerConfigurationConstants.DEFAULT_SYNC_INTERVAL_BYTES;
import static org.apache.flume.serialization.AvroEventSerializerConfigurationConstants.SYNC_INTERVAL_BYTES;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.serialization.EventSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neon.Tracker.TrackerEvent;

/**
 * <p>
 * This class serializes Flume {@linkplain Event events} into Avro data files.
 * The Flume event body is read as an Avro datum, and is then written to the
 * {@link EventSerializer}'s output stream in Avro data file format.
 * </p>
 * <p>
 * The Avro schema is determined by reading a Flume event header. The schema may
 * be specified either as a literal, by setting
 * {@link #AVRO_SCHEMA_LITERAL_HEADER} (not recommended, since the full schema
 * must be transmitted in every event), or as a URL which the schema may be read
 * from, by setting {@link #AVRO_SCHEMA_URL_HEADER}. Schemas read from URLs are
 * cached by instances of this class so that the overhead of retrieval is
 * minimized.
 * </p>
 */
public class NeonAvroEventSerializer implements EventSerializer, Configurable {

  private static final Logger logger = LoggerFactory.getLogger(NeonAvroEventSerializer.class);

  public static final String AVRO_SCHEMA_LITERAL_HEADER = "flume.avro.schema.literal";
  public static final String AVRO_SCHEMA_URL_HEADER = "flume.avro.schema.url";

  private final OutputStream out;
  private DatumWriter<Object> writer = null;
  private DataFileWriter<Object> dataFileWriter = null;

  private int syncIntervalBytes;
  private String compressionCodec;
  private Map<String, Schema> schemaCache = new HashMap<String, Schema>();

  private GenericRecord trackerEvent = null;
  private GenericDatumReader<TrackerEvent> eventReader = null;

  // State counters
  NeonAvroSerializerCounter counters = null;

  private NeonAvroEventSerializer(OutputStream out) {
    this.out = out;
  }

  @Override
  public void configure(Context context) {
    syncIntervalBytes = context.getInteger(SYNC_INTERVAL_BYTES, DEFAULT_SYNC_INTERVAL_BYTES);
    compressionCodec = context.getString(COMPRESSION_CODEC, DEFAULT_COMPRESSION_CODEC);

    if (counters == null) {
      counters = new NeonAvroSerializerCounter("avro_serializer");
      counters.start();
    }
  }

  @Override
  public void afterCreate() throws IOException {
    // no-op
  }

  @Override
  public void afterReopen() throws IOException {
    // impossible to initialize DataFileWriter without writing the schema?
    throw new UnsupportedOperationException("Avro API doesn't support append");
  }

  private void writeImpl(Event event) throws IOException {
    if (dataFileWriter == null) {
      initialize(event);
    }

    trackerEvent = null;
    Schema schema = getSchema(event);

    eventReader = new GenericDatumReader<TrackerEvent>(schema, TrackerEvent.getClassSchema());

    BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(event.getBody(), null);

    try {
      trackerEvent = eventReader.read(null, binaryDecoder);
      dataFileWriter.append(trackerEvent);
    } catch (IOException e) {
      logger.error("Error reading avro event " + e.toString());
      counters.increment(NeonHBaseSerializerCounter.COUNTER_INVALID_EVENTS);
      return;
    } catch (AvroRuntimeException e) {
      logger.error("Error parsing avro event " + e.toString());
      counters.increment(NeonHBaseSerializerCounter.COUNTER_INVALID_EVENTS);
      return;
    }
  }

  @Override
  public void write(Event event) throws IOException {
    try {
      writeImpl(event);
    } catch (FileNotFoundException e) {
      logger.error("Could not find the Avro URL file");
    } catch (Exception e) {
      logger.error("Error while writing Avro Event");
    }
  }

  private void initialize(Event event) throws IOException {
    Schema schema = TrackerEvent.getClassSchema(); // getSchema(event);

    writer = new GenericDatumWriter<Object>(schema);
    dataFileWriter = new DataFileWriter<Object>(writer);

    dataFileWriter.setSyncInterval(syncIntervalBytes);

    try {
      CodecFactory codecFactory = CodecFactory.fromString(compressionCodec);
      dataFileWriter.setCodec(codecFactory);
    } catch (AvroRuntimeException e) {
      logger.warn("Unable to instantiate avro codec with name (" + compressionCodec
          + "). Compression disabled. Exception follows.", e);
    }

    dataFileWriter.create(schema, out);
  }

  private Schema getSchema(Event event) throws IOException {
    Schema schema = null;
    String schemaUrl = event.getHeaders().get(AVRO_SCHEMA_URL_HEADER);
    if (schemaUrl != null) {
      schema = schemaCache.get(schemaUrl);
      if (schema == null) {
        schema = loadFromUrl(schemaUrl);
        schemaCache.put(schemaUrl, schema);
      }
    }
    if (schema == null) {
      String schemaString = event.getHeaders().get(AVRO_SCHEMA_LITERAL_HEADER);
      if (schemaString == null) {
        throw new FlumeException("Could not find schema for event " + event);
      }
      schema = new Schema.Parser().parse(schemaString);
    }
    return schema;
  }

  private Schema loadFromUrl(String schemaUrl) throws IOException {
    Configuration conf = new Configuration();
    Schema.Parser parser = new Schema.Parser();
    if (schemaUrl.toLowerCase().startsWith("hdfs://")) {
      FileSystem fs = FileSystem.get(conf);
      FSDataInputStream input = null;
      try {
        input = fs.open(new Path(schemaUrl));
        return parser.parse(input);
      } finally {
        if (input != null) {
          input.close();
        }
      }
    } else {
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
  }

  @Override
  public void flush() throws IOException {
    dataFileWriter.flush();
  }

  @Override
  public void beforeClose() throws IOException {
    if (counters != null) {
      counters.stop();
    }
  }

  @Override
  public boolean supportsReopen() {
    return false;
  }

  public static class Builder implements EventSerializer.Builder {

    @Override
    public EventSerializer build(Context context, OutputStream out) {
      NeonAvroEventSerializer writer = new NeonAvroEventSerializer(out);
      writer.configure(context);
      return writer;
    }

  }

}
