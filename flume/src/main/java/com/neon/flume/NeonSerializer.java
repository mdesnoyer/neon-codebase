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
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
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
    private final List<PutRequest> puts = new ArrayList<PutRequest>();
    private final List<AtomicIncrementRequest> incs = new ArrayList<AtomicIncrementRequest>();
    private byte[] currentRowKey;
    private final byte[] eventCountCol = "eventCount".getBytes();


    private String schemaUrlCurrent;
    private Schema schema;
    private GenericRecord trackerRecord;

    // private String delim;
 
    @Override
    public void initialize(byte[] table, byte[] cf) 
    {
        
        this.table = table;
        this.colFam = cf;
        
        // create the avro schema object
        schemaUrlCurrent = new String();
        Schema schema = null; 
        trackerRecord = null;
    }
 
    @Override
    public void setEvent(Event event) 
    {
        // Set the event and verify that the rowKey is not present
        
        this.currentEvent = event;

        String schemaUrl = currentEvent.getHeaders().get("flume.avro.schema.url");

        // update the schema if empty or different
        if(schemaUrl != schemaUrlCurrent) {
        
            try {
                schema = loadFromUrl(schemaUrl);
            }
            catch(IOException e) {
                this.currentEvent = null;
                return;
            }
        }

        String eventStr = new String(currentEvent.getBody());
        byte[] encodedByteArray = eventStr.getBytes();


        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(encodedByteArray, null);
        
        try {
            GenericRecord trackerRecord = reader.read(null, decoder);
        }
        catch(IOException e) {
        }
    
        //trackerRecord  = new GenericData.Record(schema);
    }
 
    @Override
    public List<PutRequest> getActions() 
    {
        return null;
    }
 
    @Override
    public List<AtomicIncrementRequest> getIncrements() 
    {
        incs.clear();
        
        //Increment the number of events received
        //incs.add(new AtomicIncrementRequest(table, "totalEvents".getBytes(), colFam, eventCountCol));
        return null;
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


    @Override
    public void cleanUp() 
    {
        
        table = null;
        colFam = null;
        currentEvent = null;
        columnNames = null;
        currentRowKey = null;
    }
 
    @Override
    public void configure(Context context) 
    {
        /*
        //Get the column names from the configuration
        String cols = new String(context.getString("columns"));
        String[] names = cols.split(",");
        columnNames = new byte[names.length][];
        int i = 0;
        
        for(String name : names) 
        {
            columnNames[i++] = name.getBytes();
        }
        */
        //delim = new String(context.getString("delimiter"));
    }
 
    @Override
    public void configure(ComponentConfiguration conf) {}
}






