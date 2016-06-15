package org.logstash.beats;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

/**
 * Created by ph on 2016-06-03.
 */
public class JsonUtils {
    public final static ObjectMapper mapper = new ObjectMapper().registerModule(new AfterburnerModule());
}
