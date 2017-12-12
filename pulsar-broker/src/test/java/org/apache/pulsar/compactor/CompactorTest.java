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
package org.apache.pulsar.compactor;

import static org.testng.Assert.assertEquals;

import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import com.google.common.util.concurrent.MoreExecutors;

public class CompactorTest extends MockedPulsarServiceBaseTest {
    private static final Logger log = LoggerFactory.getLogger(CompactorTest.class);

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
    public void testCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";
        final long numKeys = 10L;

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);


        ProducerConfiguration producerConf = new ProducerConfiguration();
        Producer producer = pulsarClient.createProducer(topic, producerConf);

        // add messages to topic, keeping latest for each key
        Map<String, byte[]> expected = new HashMap<>();
        for (int j = 0; j < 100; j++) {
            for (int i = 0; i < numKeys; i++) {
                String key = "key"+i;
                byte[] data = ("my-message-" + i + j).getBytes();
                producer.send(MessageBuilder.create()
                              .setKey(key)
                              .setContent(data).build());
                expected.put(key, data);
            }
        }

        BookKeeper bk = pulsar.getBookKeeperClientFactory().create(
                this.conf, null);
        Compactor compactor = new Compactor(pulsarClient, bk,
                                            MoreExecutors.sameThreadExecutor());
        Compactor.CompactionSpec compacted = compactor.compact(topic).get();
        CompactionNotification notification
            = new CompactionNotification(mockZookKeeper);
        notification.topicCompacted(topic, compacted);
        log.info("IKDEBUG seek to earliest");
        Consumer consumer = pulsarClient.subscribe(
                topic,
                "my-subscriber-name",
                conf);

        consumer.seek(MessageId.earliest);

        while (true) {
            Message m = consumer.receive(1, TimeUnit.SECONDS);
            log.info("IKDEBUG got key {}", m == null ? m : m.getKey());
            if (m == null) {
                break;
            }
            Assert.assertEquals(m.getData(), expected.remove(m.getKey()),
                                "Compacted version should match expected version");
        }
        log.info("expected {}", expected);
        Assert.assertTrue(expected.isEmpty(),
                          "All expected keys should have been received");
    }

    @Test
    public void testRawConsumer() throws Exception {
        final long numKeys = 10L;

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

        RawReader reader = RawReader.create(pulsarClient, topic).get();
        reader.seekAsync(MessageId.earliest).get();

        int messagesRead = 0;
        try {
            while (true) { // should break out with TimeoutException
                try (RawMessage m = reader.readNextAsync().get(1, TimeUnit.SECONDS)) {
                    messagesRead++;
                }
            }
        } catch (TimeoutException te) {
            // ok
        }

        Assert.assertEquals(messagesRead, numKeys);
    }
}
