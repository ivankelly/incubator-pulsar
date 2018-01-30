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
package org.apache.pulsar.compaction;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import java.io.FileInputStream;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.PulsarService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Properties;

import org.apache.pulsar.client.api.ClientConfiguration;
import java.nio.file.Paths;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.BookKeeperClientFactoryImpl;
import org.apache.pulsar.client.api.PulsarClient;

import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperBkClientFactoryImpl;
import org.apache.zookeeper.ZooKeeper;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import org.apache.bookkeeper.client.BookKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactorTool {

    private static class Arguments {
        @Parameter(names = {"-c", "--broker-conf"}, description = "Configuration file for Broker")
        private String brokerConfigFile = Paths.get("").toAbsolutePath().normalize().toString() + "/conf/broker.conf";

        @Parameter(names = {"-C", "--client-conf"}, description = "Configuration file for Pulsar Client")
        private String clientConfigFile = Paths.get("").toAbsolutePath().normalize().toString() + "/conf/client.conf";

        @Parameter(names = {"-t", "--topic"}, description = "Topic to compact", required=true)
        private String topic;

        @Parameter(names = {"-h", "--help"}, description = "Show this help message")
        private boolean help = false;
    }

    public static void main(String[] args) throws Exception {
        Arguments arguments = new Arguments();
        JCommander jcommander = new JCommander(arguments);
        jcommander.setProgramName("PulsarTopicCompactor");

        // parse args by JCommander
        jcommander.parse(args);
        if (arguments.help) {
            jcommander.usage();
            System.exit(-1);
        }

        // init broker config
        ServiceConfiguration brokerConfig;
        if (isBlank(arguments.brokerConfigFile)) {
            jcommander.usage();
            throw new IllegalArgumentException("Need to specify a configuration file for broker");
        } else {
            brokerConfig = PulsarConfigurationLoader.create(
                    arguments.brokerConfigFile, ServiceConfiguration.class);
        }

        String pulsarServiceUrl = PulsarService.brokerUrl(brokerConfig);
        ClientConfiguration clientConfig = new ClientConfiguration();

        if (!isNotBlank(arguments.clientConfigFile)) {
            try (FileInputStream fis = new FileInputStream(arguments.clientConfigFile)) {
                Properties properties = new Properties();
                properties.load(fis);

                if (isNotBlank(properties.getProperty("serviceUrl"))) {
                    pulsarServiceUrl = properties.getProperty("serviceUrl");
                }
                if (isNotBlank(properties.getProperty("authPlugin"))) {
                    clientConfig.setAuthentication(properties.getProperty("authPlugin"),
                                                   properties.getProperty("authParams"));
                }

                clientConfig.setUseTls(Boolean.parseBoolean(properties.getProperty("useTls")));
                clientConfig.setTlsAllowInsecureConnection(
                        Boolean.parseBoolean(properties.getProperty("tlsAllowInsecureConnection")));
                clientConfig.setTlsTrustCertsFilePath(properties.getProperty("tlsTrustCertsFilePath"));
            }
        }

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("compaction-%d").setDaemon(true).build());

        ZooKeeperClientFactory zkClientFactory = new ZookeeperBkClientFactoryImpl();

        ZooKeeper zk = zkClientFactory.create(brokerConfig.getZookeeperServers(),
                                              ZooKeeperClientFactory.SessionType.ReadWrite,
                                              (int)brokerConfig.getZooKeeperSessionTimeoutMillis()).get();
        BookKeeperClientFactory bkClientFactory = new BookKeeperClientFactoryImpl();
        BookKeeper bk = bkClientFactory.create(brokerConfig, zk);
        try (PulsarClient pulsar = PulsarClient.create(pulsarServiceUrl, clientConfig)) {
            Compactor compactor = new TwoPhaseCompactor(brokerConfig, pulsar, bk, scheduler);
            long ledgerId = compactor.compact(arguments.topic).get();
            log.info("Compaction of topic {} complete. Compacted to ledger {}", arguments.topic, ledgerId);
        } finally {
            bk.close();
            bkClientFactory.close();
            zk.close();
            scheduler.shutdownNow();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(CompactorTool.class);
}
