/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.influxdb;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.wnameless.json.flattener.JsonFlattener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;


import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Properties;

public class InfluxDBSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(com.github.jcustenborder.kafka.connect.influxdb.InfluxDBSinkTask.class);
  InfluxDBSinkConnectorConfig config;
  InfluxDBFactory factory = new InfluxDBFactoryImpl();
  InfluxDB influxDB;
  ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  @Override
  public void start(Map<String, String> settings) {
    this.config = new InfluxDBSinkConnectorConfig(settings);
    this.influxDB = this.factory.create(this.config);
  }
  static final Schema TAG_OPTIONAL_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build();
  @Override
  public void put(Collection<SinkRecord> records) {
    /**
     * Mobius CIN Return Data
     *
     * {
     *     "m2m:rce": {
     *         "uri": "Mobius/kafka_ae/t_cnt2/4-202204050238413792316",
     *         "m2m:cin": {
     *             "rn": "4-202204050238413792316",
     *             "ty": 4,
     *             "pi": "3-20220404020617497645",
     *             "ri": "4-20220405023841380700",
     *             "ct": "20220405T023841",
     *             "lt": "20220405T023841",
     *             "st": 192,
     *             "et": "20240405T023841",
     *             "cs": 62,
     *             "con": {
     *                 "Latitude": 15.45243,
     *                 "Longitude": 102.48484,
     *                 "Altitude": 15.4545
     *             },
     *             "cr": "S20170717074825768bp2l"
     *         }
     *     }
     * }
     */
    if (null == records || records.isEmpty()) {
      return;
    }
    JSONParser jParser = new JSONParser();
    Map<PointKey, Map<String, Object>> builders = new HashMap<>(records.size());
    for (SinkRecord record : records) {

      Map<String, Object> jsonMap = (Map<String, Object>) record.value();
      System.out.println("**************** \n \n \n \n \n \n****************** \n \n \n \n HERE \n **************** \n");
      System.out.println("THIS IS VALUE OF RECORDS : " + jsonMap);

      Map<String, Object> rceData = (Map<String, Object>) jsonMap.get("m2m:rce");
      String cinURI = (String) rceData.get("uri");
      String[] uriArr = cinURI.split("/");
      String measurement = "timeseries";
      final Map<String, String> tags = new HashMap<String, String>();
      tags.put("ApplicationEntity", uriArr[1]);
      tags.put("Container", uriArr[2]);
      System.out.println("THIS IS VALUE OF CONTAINER : " + tags.toString());

      final long time = record.timestamp();
      PointKey key = PointKey.of(measurement, time, tags);
      Map<String, Object> fields = builders.computeIfAbsent(key, pointKey -> new HashMap<>(100));

      Map<String, Object> dataField = (Map<String, Object>) rceData.get("m2m:cin");
      System.out.println("THIS IS VALUE OF Data Fields : " + dataField);
      try {
        /**
         * flatten nested data field & Get Parsed Creation Time
         */

        String creationTime = (String) dataField.get("ct");
        SimpleDateFormat  dateParser  = new SimpleDateFormat("yyyyMMdd'T'HHmmss");
        SimpleDateFormat  dateFormatter   = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date parsedTime = dateParser.parse(creationTime);
        creationTime = dateFormatter.format(parsedTime);

        System.out.println("THIS IS CON STRING VALUE *** : " + dataField.get("con").toString());
        String respData = objectMapper.writeValueAsString(dataField.get("con"));

        JSONObject flattenedDataField = (JSONObject) jParser.parse(JsonFlattener.flatten(respData));
        flattenedDataField.put("creation_time", creationTime);
        System.out.println("THIS IS VALUE OF FLATTENED JSON : " + flattenedDataField);

        ArrayList<String> fieldKeys = new ArrayList<String>(flattenedDataField.keySet());
        System.out.println("THIS IS VALUE OF Data Fields KEY SET : " + flattenedDataField.keySet());

        for (String fieldKey : fieldKeys) {
          String dataType = flattenedDataField.get(fieldKey).getClass().getSimpleName();
          Object o = flattenedDataField.get(fieldKey);
          System.out.println("THIS IS VALUE OF Data Fields : " + dataType + fieldKey + String.valueOf(o));

          if (o instanceof String || o instanceof Character || o instanceof Boolean || o instanceof JSONObject || o instanceof JSONArray) {
            fields.put(fieldKey, String.valueOf(o));
          } else if (o instanceof Byte || o instanceof Short || o instanceof Integer || o instanceof Long || o instanceof Double || o instanceof Float) {
            fields.put(fieldKey, Double.valueOf(String.valueOf(o)));
          } else {
            fields.put(fieldKey, String.valueOf(o));
          }
        }
      } catch (ParseException e) {
        e.printStackTrace();
      } catch (java.text.ParseException e) {
        e.printStackTrace();
      } catch (JsonMappingException e) {
        e.printStackTrace();
      } catch (JsonParseException e) {
        e.printStackTrace();
      } catch (JsonProcessingException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    /*
     * For Kafka Produce (Flatten Data)
     */
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    Producer<String, String> producer = new KafkaProducer<String, String>(props);
    Map<String, Object> flattenData = null;

    BatchPoints.Builder batchBuilder = BatchPoints.database(this.config.database).consistency(this.config.consistencyLevel);

    for (Map.Entry<PointKey, Map<String, Object>> values : builders.entrySet()) {
      final Point.Builder builder = Point.measurement(values.getKey().measurement);
      builder.time(values.getKey().time, this.config.precision);
      if (null != values.getKey().tags || values.getKey().tags.isEmpty()) {
        builder.tag(values.getKey().tags);
        flattenData = values.getValue();
      }
      builder.fields(values.getValue());
      Point point = builder.build();
      if (log.isTraceEnabled()) {
        log.trace("put() - Adding point {}", point.toString());
      }
      batchBuilder.point(point);
      Map<String, String> tmpTags = values.getKey().tags;
      String kafkaTopic = "refine." + tmpTags.get("ApplicationEntity") + "." + tmpTags.get("Container");
      Map<String, Object> kafkaData = flattenData;
      kafkaData.put("ApplicationEntity", values.getKey().tags.get("ApplicationEntity"));
      kafkaData.put("container", values.getKey().tags.get("Container"));
      try {
        producer.send(new ProducerRecord<String, String>(kafkaTopic, objectMapper.writeValueAsString(kafkaData))); //topic, data
        System.out.println("Message sent successfully" + flattenData);
        producer.close();
      } catch (Exception e) {
        System.out.println("Kafka Produce Exception : " + e);
      }

    }
    BatchPoints batch = batchBuilder.build();
    this.influxDB.write(batch);
  }

  @Override
  public void stop() {
    if (null != this.influxDB) {
      log.info("stop() - Closing InfluxDB client.");
      this.influxDB.close();
    }
  }
}