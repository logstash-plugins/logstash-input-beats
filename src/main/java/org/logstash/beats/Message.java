package org.logstash.beats;

import java.util.HashMap;
import java.util.Map;

public class Message implements Comparable<Message> {
    private final int sequence;
    private String identityStream;
    private final Map data;
    private Batch batch;

    public Message(int sequence, Map map) {
        this.sequence = sequence;
        this.data = map;

        identityStream = extractIdentityStream();
    }

    public int getSequence() {
        return sequence;
    }


    public Map getData() {
        return data;
    }

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

    public String getIdentityStream() {
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
                return (String) beatsData.get("name") + "-" + (String) beatsData.get("source");
            }
        }

        return null;
    }
}