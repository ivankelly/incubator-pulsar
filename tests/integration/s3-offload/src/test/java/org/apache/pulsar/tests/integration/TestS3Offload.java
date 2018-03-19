/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.pulsar.tests.integration;

import com.github.dockerjava.api.DockerClient;

import java.net.URL;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;

import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.tests.DockerUtils;
import org.apache.pulsar.tests.PulsarClusterUtils;

import org.jboss.arquillian.testng.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestS3Offload extends Arquillian {
    private static final Logger LOG = LoggerFactory.getLogger(TestS3Offload.class);
    private static byte[] PASSWD = "foobar".getBytes();
    private static String clusterName = "test";

    @ArquillianResource
    DockerClient docker;

    public static void updateConf(DockerClient docker, String containerId,
                                        String confFile, String key, String value) throws Exception {
        String sedProgram = String.format(
                "/[[:blank:]]*%s[[:blank:]]*=/ { h; s!=.*!=%s!; }; ${x;/^$/ { s//%s=%s/;H; }; x}",
                key, value, key, value);
        DockerUtils.runCommand(docker, containerId, "sed", "-i", "-e", sedProgram, confFile);
    }

    @Test
    public void testPublishOffloadAndConsume() throws Exception {
        int entriesPerLedger = 10;
        for (String b : PulsarClusterUtils.brokerSet(docker, "test")) {
            updateConf(docker, b, "/pulsar/conf/broker.conf",
                       "managedLedgerMaxEntriesPerLedger", String.valueOf(entriesPerLedger));
            updateConf(docker, b, "/pulsar/conf/broker.conf",
                       "managedLedgerMinLedgerRolloverTimeMinutes", "0");
        }

        Assert.assertTrue(PulsarClusterUtils.startAllBrokers(docker, clusterName));
        Assert.assertTrue(PulsarClusterUtils.startAllProxies(docker, clusterName));

        // create property and namespace
        PulsarClusterUtils.runOnAnyBroker(docker, clusterName,
                "/pulsar/bin/pulsar-admin", "properties",
                "create", "smoke-test", "--allowed-clusters", clusterName,
                "--admin-roles", "smoke-admin");
        PulsarClusterUtils.runOnAnyBroker(docker, clusterName,
                "/pulsar/bin/pulsar-admin", "namespaces",
                "create", "smoke-test/test/ns1");

        String brokerIp = DockerUtils.getContainerIP(
                docker, PulsarClusterUtils.proxySet(docker, clusterName).stream().findAny().get());
        String serviceUrl = "pulsar://" + brokerIp + ":6650";
        String topic = "persistent://smoke-test/test/ns1/topic1";

        String brokerIp2 = DockerUtils.getContainerIP(
                docker, PulsarClusterUtils.brokerSet(docker, clusterName).stream().findAny().get());
        String serviceUrl2 = "http://" + brokerIp2 + ":8080";

        ClientConfiguration bkConf = new ClientConfiguration();
        bkConf.setZkServers(PulsarClusterUtils.zookeeperConnectString(docker, "test"));

        try(PulsarClient client = PulsarClient.create(serviceUrl);
            Consumer consumer = client.subscribe(topic, "my-sub");
            Producer producer = client.createProducer(topic)) {
            // write enough to topic to make it roll
            for (int i = 0; i < entriesPerLedger*1.5; i++) {
                producer.send(("smoke-message"+i).getBytes());
            }

            // read managed ledger info, check ledgers exist
            ManagedLedgerFactory mlf = new ManagedLedgerFactoryImpl(bkConf);
            ManagedLedgerInfo info = mlf.getManagedLedgerInfo("smoke-test/test/ns1/persistent/topic1");
            Assert.assertEquals(info.ledgers.size(), 2);

            // trigger offload
            try (PulsarAdmin admin = new PulsarAdmin(new URL(serviceUrl2), "", "")) {
                admin.persistentTopics().offloadToS3(topic);
            }

            // reboot broker to clear cache
            Assert.assertTrue(PulsarClusterUtils.stopAllBrokers(docker, clusterName));
            Assert.assertTrue(PulsarClusterUtils.startAllBrokers(docker, clusterName));

            // verify ledger is deleted
            try (BookKeeper bk = new BookKeeper(bkConf)) {
                try {
                    bk.openLedger(info.ledgers.get(0).ledgerId,
                                  BookKeeper.DigestType.CRC32, "".getBytes()).close();
                    Assert.fail("Shouldn't be able to open first ledger");
                } catch (BKException.BKNoSuchLedgerExistsException bke) {
                    // expected
                }

                bk.openLedger(info.ledgers.get(1).ledgerId,
                              BookKeeper.DigestType.CRC32, "".getBytes()).close();
            }

            // read back from topic
            for (int i = 0; i < entriesPerLedger*1.5; i++) {
                Message m = consumer.receive();
                Assert.assertEquals("smoke-message"+i, new String(m.getData()));
            }
        }

        PulsarClusterUtils.stopAllProxies(docker, clusterName);
        Assert.assertTrue(PulsarClusterUtils.stopAllBrokers(docker, clusterName));

        java.io.File f = new java.io.File("/tmp/pause");
        f.createNewFile();
        while (f.exists()) { Thread.sleep(1000); }
    }
}
