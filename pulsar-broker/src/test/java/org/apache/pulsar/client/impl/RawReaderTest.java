/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.api.RawReader;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PropertyAdmin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RawReaderTest extends MockedPulsarServiceBaseTest {
    private static final Logger log = LoggerFactory.getLogger(RawReaderTest.class);

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("use",
                new ClusterData("http://127.0.0.1:" + BROKER_WEBSERVICE_PORT));
        admin.properties().createProperty("my-property",
                new PropertyAdmin(Lists.newArrayList("appid1", "appid2"), Sets.newHashSet("use")));
        admin.namespaces().createNamespace("my-property/use/my-ns");
    }

    @AfterMethod
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testRawReader() throws Exception {
        int numKeys = 10;

        String topic = "persistent://my-property/use/my-ns/my-raw-topic";

        ProducerConfiguration producerConf = new ProducerConfiguration();
        Producer producer = pulsarClient.createProducer(topic, producerConf);

        Set<String> keys = new HashSet<>();
        // add messages to topic, keeping latest for each key
        Map<String, byte[]> expected = new HashMap<>();
        for (int i = 0; i < numKeys; i++) {
            String key = "key"+i;
            byte[] data = ("my-message-" + i).getBytes();
            producer.send(MessageBuilder.create()
                          .setKey(key)
                          .setContent(data).build());
            keys.add(key);
        }

        RawReader reader = RawReader.create(pulsarClient, topic).get();
        try {
            while (true) { // should break out with TimeoutException
                try (RawMessage m = reader.readNextAsync().get(1, TimeUnit.SECONDS)) {
                    ByteBuf headersAndPayload = m.getHeadersAndPayload();
                    MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
                    Assert.assertTrue(keys.remove(msgMetadata.getPartitionKey()));
                }
            }
        } catch (TimeoutException te) {
            // ok
        }

        Assert.assertTrue(keys.isEmpty());
    }

    @Test
    public void testSeekToStart() throws Exception {
        int numKeys = 10;
        String topic = "persistent://my-property/use/my-ns/my-raw-topic";

        ProducerConfiguration producerConf = new ProducerConfiguration();
        Producer producer = pulsarClient.createProducer(topic, producerConf);

        // add messages to topic, keeping latest for each key
        Map<String, byte[]> expected = new HashMap<>();
        for (int i = 0; i < numKeys; i++) {
            String key = "key"+i;
            byte[] data = ("my-message-" + i).getBytes();
            producer.send(MessageBuilder.create()
                          .setKey(key)
                          .setContent(data).build());
        }

        Set<String> readKeys = new HashSet<>();
        RawReader reader = RawReader.create(pulsarClient, topic).get();
        try {
            while (true) { // should break out with TimeoutException
                try (RawMessage m = reader.readNextAsync().get(1, TimeUnit.SECONDS)) {
                    ByteBuf headersAndPayload = m.getHeadersAndPayload();
                    MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
                    readKeys.add(msgMetadata.getPartitionKey());
                }
            }
        } catch (TimeoutException te) {
            // ok
        }
        Assert.assertEquals(readKeys.size(), numKeys);

        log.info("IKDEBUG read keys before is {}", readKeys);

        // seek to start, read all keys again,
        // assert that we read all keys we had read previously
        reader.seekAsync(MessageId.earliest).get();
        try {
            while (true) { // should break out with TimeoutException
                try (RawMessage m = reader.readNextAsync().get(1, TimeUnit.SECONDS)) {
                    ByteBuf headersAndPayload = m.getHeadersAndPayload();
                    MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
                    log.info("IKDEBUG Second pass, read key {}", msgMetadata.getPartitionKey());
                    Assert.assertTrue(readKeys.remove(msgMetadata.getPartitionKey()));
                }
            }
        } catch (TimeoutException te) {
            // ok
        }
        log.info("IKDEBUG read keys is {}", readKeys);
        Assert.assertTrue(readKeys.isEmpty());
    }

    @Test
    public void testSeekToMiddle() throws Exception {
        int numKeys = 10;
        String topic = "persistent://my-property/use/my-ns/my-raw-topic";

        ProducerConfiguration producerConf = new ProducerConfiguration();
        Producer producer = pulsarClient.createProducer(topic, producerConf);

        // add messages to topic, keeping latest for each key
        Map<String, byte[]> expected = new HashMap<>();
        for (int i = 0; i < numKeys; i++) {
            String key = "key"+i;
            byte[] data = ("my-message-" + i).getBytes();
            producer.send(MessageBuilder.create()
                          .setKey(key)
                          .setContent(data).build());
        }

        Set<String> readKeys = new HashSet<>();
        RawReader reader = RawReader.create(pulsarClient, topic).get();
        int i = 0;
        MessageId seekTo = null;
        try {
            while (true) { // should break out with TimeoutException
                try (RawMessage m = reader.readNextAsync().get(1, TimeUnit.SECONDS)) {
                    i++;
                    ByteBuf headersAndPayload = m.getHeadersAndPayload();
                    MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
                    if (i > numKeys/2) {
                        if (seekTo == null) {
                            seekTo = m.getMessageId();
                        }
                        readKeys.add(msgMetadata.getPartitionKey());
                    }
                }
            }
        } catch (TimeoutException te) {
            // ok
        }
        Assert.assertEquals(readKeys.size(), numKeys/2);

        log.info("IKDEBUG read keys before seeking to {} is {}", seekTo, readKeys);

        // seek to middle, read all keys again,
        // assert that we read all keys we had read previously
        reader.seekAsync(seekTo).get();
        try {
            while (true) { // should break out with TimeoutException
                try (RawMessage m = reader.readNextAsync().get(1, TimeUnit.SECONDS)) {
                    ByteBuf headersAndPayload = m.getHeadersAndPayload();
                    MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
                    log.info("IKDEBUG Second pass, read key {}", msgMetadata.getPartitionKey());
                    Assert.assertTrue(readKeys.remove(msgMetadata.getPartitionKey()));
                }
            }
        } catch (TimeoutException te) {
            // ok
        }
        log.info("IKDEBUG read keys is {}", readKeys);
        Assert.assertTrue(readKeys.isEmpty());
    }

    /**
     * Try to fill the receiver queue, and drain it multiple times
     */
    @Test
    public void testFlowControl() throws Exception {
        int numMessages = RawReaderImpl.DEFAULT_RECEIVER_QUEUE_SIZE * 5;
        String topic = "persistent://my-property/use/my-ns/my-raw-topic";

        ProducerConfiguration producerConf = new ProducerConfiguration();
        Producer producer = pulsarClient.createProducer(topic, producerConf);

        // add messages to topic, keeping latest for each key
        Map<String, byte[]> expected = new HashMap<>();
        for (int i = 0; i < numMessages; i++) {
            String key = "key"+i;
            byte[] data = ("my-message-" + i).getBytes();
            producer.send(MessageBuilder.create()
                          .setKey(key)
                          .setContent(data).build());
        }
        RawReader reader = RawReader.create(pulsarClient, topic).get();
        List<Future<RawMessage>> futures = new ArrayList<>();
        Set<String> keys = new HashSet<>();

        // +1 to make sure we read past the end
        for (int i = 0; i < numMessages + 1; i++) {
            futures.add(reader.readNextAsync());
        }
        int timeouts = 0;
        for (Future<RawMessage> f : futures) {
            try (RawMessage m = f.get(1, TimeUnit.SECONDS)) {
                ByteBuf headersAndPayload = m.getHeadersAndPayload();
                MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
                // Assert each key is unique
                Assert.assertTrue(keys.add(msgMetadata.getPartitionKey()));
            } catch (TimeoutException te) {
                timeouts++;
            }
        }
        Assert.assertEquals(timeouts, 1);
        Assert.assertEquals(keys.size(), numMessages);
    }
}
