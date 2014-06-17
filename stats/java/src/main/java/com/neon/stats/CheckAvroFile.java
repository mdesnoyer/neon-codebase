package com.neon.stats;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import com.neon.Tracker.TrackerEvent;

public class CheckAvroFile {

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    // Deserialize data from disk
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(TrackerEvent.getClassSchema());
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File(args[0]), datumReader);
    GenericRecord data = null;
    while (dataFileReader.hasNext()) {
      // Reuse user object by passing it to next(). This saves us from
      // allocating and garbage collecting many objects for files with
      // many items.
      data = dataFileReader.next(data);
      System.out.println(data);
    }

  }

}
