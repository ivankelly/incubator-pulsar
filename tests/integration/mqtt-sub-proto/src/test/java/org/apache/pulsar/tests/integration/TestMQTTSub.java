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
package org.apache.pulsar.tests.integration;

import com.github.dockerjava.api.DockerClient;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.tests.DockerUtils;
import org.apache.pulsar.tests.PulsarClusterUtils;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.arquillian.testng.Arquillian;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class TestMQTTSub extends Arquillian {
    private static final Logger log = LoggerFactory.getLogger(TestMQTTSub.class);
    private static String clusterName = "test";

    @ArquillianResource
    DockerClient docker;

    @BeforeMethod
    public void waitServicesUp() throws Exception {
        Assert.assertTrue(PulsarClusterUtils.waitZooKeeperUp(docker, clusterName, 30, TimeUnit.SECONDS));
        Assert.assertTrue(PulsarClusterUtils.waitAllBrokersUp(docker, clusterName));
    }

    @Test
    public void testPublishCompactViaPaho() throws Exception {
        String proxyIp = DockerUtils.getContainerIP(
                docker, PulsarClusterUtils.proxySet(docker, clusterName).stream().findAny().get());
        String serviceUrl = "pulsar://" + proxyIp + ":6650";
        String topic = "persistent://public/default/mqtt-topic1";

        String mqttBroker = "tcp://" + proxyIp + ":1833";

        MqttClient mqttClient = new MqttClient(mqttBroker, "testClient", new MemoryPersistence());
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        log.info("Connecting");
        mqttClient.connect(connOpts);

        log.info("subscribing");
        CompletableFuture<MqttMessage> promise = new CompletableFuture<>();
        mqttClient.subscribe("mqtt-topic1", (_topic, message) -> {
                log.info("got a message");
                promise.complete(message);
            });

        log.info("subscribed");

        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build()) {
            try(Producer<byte[]> producer = client.newProducer().topic(topic).create()) {
                producer.send(MessageBuilder.create().setKey("key0").setContent("content0".getBytes()).build());
                producer.send(MessageBuilder.create().setKey("key0").setContent("content1".getBytes()).build());
            }
        }

        log.info("Got message {}", promise.get());
    }


}
