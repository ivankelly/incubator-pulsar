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

import java.util.function.Consumer;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedOutputStream;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.api.proto.PulsarApi.CompactionSpec;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import com.google.protobuf.ByteString;
import java.util.Base64;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionNotification {
    private static final Logger log
        = LoggerFactory.getLogger(CompactionNotification.class);

    final ZooKeeper zookeeper;

    public CompactionNotification(ZooKeeper zookeeper) {
        this.zookeeper = zookeeper;
    }

    private static String topicPath(String topic) {
        return "/" + Base64.getEncoder().encodeToString(topic.getBytes());
    }

    public void topicCompacted(String topic, Compactor.CompactionSpec spec)
            throws Exception {
        CompactionSpec protoSpec = CompactionSpec.newBuilder()
            .setCursor(ByteString.copyFrom(spec.position.toByteArray()))
            .setLedger(spec.ledgerId).build();
        int size = protoSpec.getSerializedSize();
        ByteBuf serialized = Unpooled.buffer(size, size);
        ByteBufCodedOutputStream stream = ByteBufCodedOutputStream.get(serialized);
        protoSpec.writeTo(stream);

        try {
            zookeeper.setData(topicPath(topic),
                              serialized.array(),
                              -1);
        } catch (KeeperException.NoNodeException nne) {
            log.info("IKDEBUG nne, creating node");
            zookeeper.create(topicPath(topic),
                             serialized.array(),
                             ZooDefs.Ids.OPEN_ACL_UNSAFE,
                             CreateMode.PERSISTENT);
        }
    }

    public void getCompacted(String topic,
                             Consumer<Compactor.CompactionSpec> consumer) {
        log.info("IKDEBUG getcompacted");
        Watcher w = (e) -> {
            log.info("IKDEBUG notified of event {}", e);
            if (e.getType() == Watcher.Event.EventType.NodeCreated
                || e.getType() == Watcher.Event.EventType.NodeDataChanged) {
                getCompacted(topic, consumer);
            }
        };
        log.info("IKDEBUG 1-  mock is {}", zookeeper);
        zookeeper.getData(
                topicPath(topic),
                w,
                (rc, path, ctx, data, stat) -> {
                    log.info("IKDEBUG getdata complete {}", rc);
                    if (rc == 0) {
                        try {
                            ByteBufCodedInputStream inputStream = ByteBufCodedInputStream.get(Unpooled.wrappedBuffer(data, 0, data.length));

                            CompactionSpec spec = CompactionSpec.newBuilder()
                                .mergeFrom(inputStream, null).build();
                            consumer.accept(
                                    new Compactor.CompactionSpec(
                                            (MessageIdImpl)MessageIdImpl.fromByteArray(
                                                    spec.getCursor().toByteArray()),
                                            spec.getLedger()));
                        } catch (Exception e) {
                            log.error("Error reading compaction spec", e);
                        }
                    } else if (rc == KeeperException.Code.NONODE.intValue()) {
                        String path2 = topicPath(topic);
                        log.info("IKDEBUG 2-  mock is {}", zookeeper);
                        log.info("IKDEBUG Calling exists {}", path2);
                        try {
                                zookeeper.exists(path2,
                                         w,
                                         (rc1, path1, stat1, ctx1) -> {
                                             log.info(" IKDEBUG exists complete");
                                             if (rc1 == 0) {
                                                 getCompacted(topic, consumer);
                                             }
                                         }, null);
                        } catch (Exception e ) {
                            log.info("Ah shite", e);
                        }
                    }
                }, null);
    }
}

