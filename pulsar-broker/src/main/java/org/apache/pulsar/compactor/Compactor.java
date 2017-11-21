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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.common.api.DoubleByteBuf;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImplAccessor;
import org.apache.pulsar.common.api.proto.PulsarApi.CompactedMessage;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedOutputStream;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class Compactor {
    private static final Logger log = LoggerFactory.getLogger(Compactor.class);

    final ExecutorService executor;
    final PulsarClient pulsar;
    final BookKeeper bk;

    public Compactor(PulsarClient pulsar,
                     BookKeeper bk,
                     ExecutorService executor) {
        this.executor = executor;
        this.pulsar = pulsar;
        this.bk = bk;
    }

    public CompletableFuture<Long> compact(String topic) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        executor.submit(new CompactionTask(topic, future));
        return future;
    }

    private class CompactionTask implements Runnable {
        final String topic;
        final CompletableFuture<Long> future;


        CompactionTask(String topic, CompletableFuture<Long> future) {
            this.topic = topic;
            this.future = future;
        }

        @Override
        public void run() {
            try {
                ConsumerConfiguration conf = new ConsumerConfiguration();
                conf.setSubscriptionType(SubscriptionType.Exclusive);

                Consumer consumer = pulsar.subscribe(topic,
                                                     "__compaction",
                                                     conf);
                consumer.seek(MessageId.earliest);

                MessageId lastMessageId = null;
                MessageId firstId = null;
                Map<String,MessageId> latestForKey = new HashMap<>();
                while (!consumer.hasReachedEndOfTopic()) {
                    Message m = consumer.receive(1, TimeUnit.SECONDS);
                    if (m == null) {
                        break;
                    }
                    lastMessageId = m.getMessageId();
                    if (firstId == null) {
                        firstId = m.getMessageId();
                    }
                    latestForKey.put(m.getKey(),
                                     m.getMessageId());
                }

                if (lastMessageId == null) {
                    future.completeExceptionally(
                            new Exception("Nothing to compact"));
                } else {
                    LedgerHandle lh = bk.createLedger(3, 3,
                            BookKeeper.DigestType.CRC32,
                            "".getBytes());
                    consumer.seek(firstId);
                    MessageId current = firstId;
                    CompletableFuture<Void> lastFuture = null;
                    do {
                        Message m = consumer.receive();
                        current = m.getMessageId();
                        if (latestForKey.get(m.getKey()).equals(m.getMessageId())) {
                            ByteBuf serialized = serializeMessage(m);
                            final CompletableFuture<Void> f
                                = new CompletableFuture<>();
                            lh.asyncAddEntry(serialized,
                                    (rc, ledger, id, ctx) -> {
                                                 if (rc != BKException.Code.OK) {
                                                     f.completeExceptionally(BKException.create(rc));
                                                 } else {
                                                     f.complete(null);
                                                 }
                                             }, null);
                            lastFuture = f;
                            serialized.release();
                        }
                    } while (!current.equals(lastMessageId));

                    if (lastFuture != null) {
                        lastFuture.get();
                    }
                    lh.close();
                    future.complete(lh.getId());
                }
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        }
    }

    private static ByteBuf serializeMessage(Message m) throws IOException {
        // copy out common code from ProducerImpl
        MessageIdImpl id = (MessageIdImpl)m.getMessageId();
        CompactedMessage.Builder builder = CompactedMessage.newBuilder()
            .setMetadata(MessageImplAccessor.getMetadata(m));
        builder.setId(MessageIdData.newBuilder()
                      .setLedgerId(id.getLedgerId())
                      .setEntryId(id.getEntryId())
                      .setPartition(id.getPartitionIndex())
                      .build());

        CompactedMessage compacted = builder.build();
        int size = compacted.getSerializedSize() + 4;
        ByteBuf headers = PooledByteBufAllocator.DEFAULT.buffer(size, size);
        headers.writeInt(compacted.getSerializedSize());
        ByteBufCodedOutputStream outStream
            = ByteBufCodedOutputStream.get(headers);
        compacted.writeTo(outStream);
        outStream.recycle();
        ByteBuf payload = MessageImplAccessor.getPayload(m);
        return DoubleByteBuf.get(headers, payload);
    }

    public static Message deserializeMessage(ByteBuf buf) throws IOException {
        CompactedMessage.Builder builder = CompactedMessage.newBuilder();
        int size = (int) buf.readUnsignedInt();
        int writerIndex = buf.writerIndex();
        buf.writerIndex(buf.readerIndex() + size);
        ByteBufCodedInputStream inStream = ByteBufCodedInputStream.get(buf);
        CompactedMessage compacted = builder.mergeFrom(inStream, null).build();
        buf.writerIndex(writerIndex);
        ByteBuf payload = buf.copy();

        return MessageImplAccessor.newMessage(compacted.getId(),
                                              compacted.getMetadata(),
                                              payload);
    }
}
