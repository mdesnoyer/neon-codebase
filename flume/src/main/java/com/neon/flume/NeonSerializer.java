package com.neon.flume;


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
    private byte[] table;
    private byte[] colFam;
    private Event currentEvent;
    private byte[][] columnNames;
    private final List<PutRequest> actions = new ArrayList<PutRequest>();
    private final List<AtomicIncrementRequest> increments = new ArrayList<AtomicIncrementRequest>();
    private byte[] currentRowKey;
    private final byte[] eventCountCol = "eventCount".getBytes();

    private final String timestampColumnName = new String("timestamp");
    private final String counterColumnName = new String("counter");

    // event data 
    private TrackerEvent trackerEvent;
    private String trackerEventTimestamp;
    private String rowKey;

    @Override
    public void initialize(byte[] table, byte[] cf) 
    {
        this.table = table;
        this.colFam = cf;
        trackerEvent = null;
        trackerEventTimestamp = null;
    }
 
    @Override
    public void setEvent(Event event) 
    {
        this.currentEvent = event;

        try {

            // obtain the timestamps of event
            String t = event.getHeaders().get("timestamp");
            long timestamp = Long.valueOf(t).longValue();

            // remove minutes and seconds precision
            timestamp %= 3600;
            trackerEventTimestamp = Long.toString(timestamp); 

            // obtain the tracker event
            SeekableByteArrayInput input = new SeekableByteArrayInput(currentEvent.getBody() );  
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
        // if this event was dropped previously do nothing
        if(this.currentEvent == null)
            return null;
 
        actions.clear();

        switch(trackerEvent.getEventType())  {

            case IMAGE_VISIBLE: 
                ImageVisible data = (ImageVisible) trackerEvent.getEventData();
                handleImageVisibleEvent(data);
                break;

            default:
                this.currentEvent = null;
                return null;
        }

        return actions;
    }
 

    private void handleImageVisibleEvent(ImageVisible img) {

        rowKey = img.getThumbnailId().toString();
        
        // timestamps colum 
        PutRequest req = new PutRequest(table, rowKey.getBytes(), colFam, columnNames[0], trackerEventTimestamp.getBytes());
        actions.add(req);
    }

    @Override
    public List<AtomicIncrementRequest> getIncrements() 
    {
        // if this event was dropped in previously do nothing 
        if(this.currentEvent == null)
            return null;

        increments.clear();
        
        //Increment the number of events by one implicitly
        increments.add(new AtomicIncrementRequest(table, rowKey.getBytes(), colFam, columnNames[1]));
        return null;
    }

    /*
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
   */

    @Override
    public void cleanUp() 
    {
        table = null;
        colFam = null;
        currentEvent = null;
        columnNames = null;
        trackerEvent = null;
    }
 
    @Override
    public void configure(Context context) 
    {
        
        //Get the column names from the configuration
        String cols = new String(context.getString("columns"));
        String[] names = cols.split(",");
        columnNames = new byte[names.length][];
        int i = 0;
        
        for(String name : names) 
        {
            columnNames[i++] = name.getBytes();
        }
    }
 
    @Override
    public void configure(ComponentConfiguration conf) {}


   /* 
    private boolean schemaFetchAllowed() {

        long elapsed = System.nanoTime() - lastSchemaFetchTime;

        if(elapsed >= minimumNanoTimeBetweenFetch)
            return true;
        
        return false;
    }
   */

}






