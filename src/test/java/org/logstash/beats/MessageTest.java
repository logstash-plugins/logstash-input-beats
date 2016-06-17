package org.logstash.beats;

import org.junit.Test;
import org.logstash.beats.Message;

import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


public class MessageTest {
    @Test
    public void TestGetData() {
        Map<Object, Object> map = new HashMap();

        Message message = new Message(1, map);
        assertEquals(map, message.getData());
    }

    @Test
    public void TestGetSequence() {
        Map<Object, Object> map = new HashMap();

        Message message = new Message(1, map);
        assertEquals(1, message.getSequence());
    }

    @Test
    public void TestComparison() {
        Map<Object, Object> map = new HashMap();
        Message messageOlder = new Message(1, map);
        Message messageNewer = new Message(2, map);

        assertThat(messageNewer, is(greaterThan(messageOlder)));
    }

    @Test
    public void TesGenerateAnIdentityStreamWhenIdAndResourceArePresent() {
        Map<Object, Object> map = new HashMap();
        Map<String, String> beatsData = new HashMap();

        beatsData.put("id", "uuid1234");
        beatsData.put("resource_id", "rid1234");

        map.put("beat", beatsData);
        Message message = new Message(1, map);

        assertEquals("uuid1234-rid1234", message.getIdentityStream());
    }

    @Test
    public void TesGenerateAnIdentityStreamWhenResourceIdIsAbsent() {
        Map<Object, Object> map = new HashMap();
        Map<String, String> beatsData = new HashMap();

        beatsData.put("id", "uuid1234");
        beatsData.put("name", "filebeat");
        beatsData.put("source", "/var/log/message.log");

        map.put("beat", beatsData);
        Message message = new Message(1, map);

        assertEquals("filebeat-/var/log/message.log", message.getIdentityStream());
    }


    @Test
    public void TesGenerateAnIdentityStreamWhenIdIsAbsent() {
        Map<Object, Object> map = new HashMap();
        Map<String, String> beatsData = new HashMap();

        beatsData.put("resource_id", "rid1234");
        beatsData.put("name", "filebeat");
        beatsData.put("source", "/var/log/message.log");

        map.put("beat", beatsData);
        Message message = new Message(1, map);

        assertEquals("filebeat-/var/log/message.log", message.getIdentityStream());
    }

    @Test
    public void TesGenerateAnIdentityStreamWhenIdAndResourceIdareAbsent() {
        Map<Object, Object> map = new HashMap();

        Message message = new Message(1, map);

        assertNull(message.getIdentityStream());
    }
}