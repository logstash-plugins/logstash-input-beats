package org.logstash.beats;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Message implements Comparable<Message> {
    private final int sequence;
    private String identityStream;
    private Map data;
    private Batch batch;
    private ByteBuf buffer;
    private Logger logger = LogManager.getLogger(Message.class);

    public final static ObjectMapper MAPPER = new ObjectMapper().registerModule(new AfterburnerModule());

    public Message(int sequence, Map map) {
        this.sequence = sequence;
        this.data = map;
    }

    public Message(int sequence, ByteBuf buffer){
        this.sequence = sequence;
        this.buffer = buffer;
    }

    public int getSequence() {
        return sequence;
    }

    public boolean release() {
        if (buffer != null){
            return buffer.release();
        }
        return true;
    }

    public Map getData(){
        if (data == null && buffer != null){
            try (ByteBufInputStream byteBufInputStream = new ByteBufInputStream(buffer)){
                data = (Map)MAPPER.readValue(byteBufInputStream, Object.class);
            } catch (IOException e){
                throw new RuntimeException("Unable to parse beats payload ", e);
            }
//            finally{
//                release();
//            }
        }

        return data;
    }

    private NewBatch newBatch;

    @Override
    public int compareTo(Message o) {
        return Integer.compare(getSequence(), o.getSequence());
    }

    public Batch getBatch() {
        return batch;
    }

    public void setBatch(Batch newBatch) {
        batch = newBatch;
    }

    public NewBatch getNewBatch() {
        return newBatch;
    }

    public void setNewBatch(NewBatch newnewBatch){
        this.newBatch = newnewBatch;
    }


    public String getIdentityStream() {
        if (identityStream == null){
            identityStream = extractIdentityStream();
        }
        return identityStream;
    }

    private String extractIdentityStream() {
        Map beatsData = (HashMap<String, String>) this.getData().get("beat");

        if(beatsData != null) {
            String id = (String) beatsData.get("id");
            String resourceId = (String) beatsData.get("resource_id");

            if(id != null && resourceId != null) {
                return id + "-" + resourceId;
            } else {
                return beatsData.get("name") + "-" + beatsData.get("source");
            }
        }

        return null;
    }
}