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
package org.apache.bookkeeper.mledger.impl;

import io.netty.buffer.Unpooled;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;


import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.shaded.com.google.protobuf.ByteString;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat;

class S3Offloader {
    private final ExecutorService executor;
    private final AmazonS3 s3;

    private final String BUCKET_NAME = "pulsar-offload";
    private final String REGION = "eu-west-3";

    S3Offloader(ExecutorService executor) throws AmazonS3Exception {
        this.executor = executor;
        s3 = AmazonS3ClientBuilder.standard()
            .withCredentials(new ProfileCredentialsProvider("default")) // make sure ~/.aws/credentials is configured
            .withRegion(REGION).build();

        if (!s3.doesBucketExist(BUCKET_NAME)) {
            s3.createBucket(BUCKET_NAME);
        }
    }

    public CompletableFuture<String> offload(ReadHandle ledger) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(baos);

        LedgerMetadata metadata = ledger.getLedgerMetadata();

        LedgerMetadataFormat.Builder builder = LedgerMetadataFormat.newBuilder();
        builder.setQuorumSize(metadata.getWriteQuorumSize())
            .setAckQuorumSize(metadata.getAckQuorumSize())
            .setEnsembleSize(metadata.getEnsembleSize())
            .setLength(metadata.getLength())
            .setCtime(metadata.getCtime())
            .setLastEntryId(metadata.getLastEntryId())
            .setDigestType(LedgerMetadataFormat.DigestType.CRC32)
            .setState(metadata.isClosed() ? LedgerMetadataFormat.State.CLOSED : LedgerMetadataFormat.State.OPEN);

        for (Map.Entry<String, byte[]> e : metadata.getCustomMetadata().entrySet()) {
            builder.addCustomMetadataBuilder()
                .setKey(e.getKey()).setValue(ByteString.copyFrom(e.getValue()));
        }

        for (Map.Entry<Long, ? extends List<BookieSocketAddress>> e : metadata.getAllEnsembles().entrySet()) {
            builder.addSegmentBuilder()
                .setFirstEntryId(e.getKey())
                .addAllEnsembleMember(e.getValue().stream().map(a -> a.toString()).collect(Collectors.toList()));
        }

        CompletableFuture<String> promise = new CompletableFuture<>();
        try {
            data.writeLong(ledger.getId());
            builder.build().writeDelimitedTo(data);
            data.writeLong(ledger.getLastAddConfirmed()+1);

            ledger.read(0, ledger.getLastAddConfirmed())
                .whenComplete((entries, exception) -> {
                        try {
                            if (exception != null) {
                                promise.completeExceptionally(exception);
                                data.close();
                                return;
                            }
                            try {
                                for (LedgerEntry e : entries) {
                                    byte[] bytes = e.getEntryBytes();
                                    data.writeLong(bytes.length);
                                    data.write(bytes, 0, bytes.length);
                                }

                                byte[] objectBytes = baos.toByteArray();
                                ObjectMetadata om = new ObjectMetadata();
                                om.setContentLength(objectBytes.length);

                                String name = "ledger-" + ledger.getId();
                                s3.putObject(BUCKET_NAME,
                                             name,
                                             new ByteArrayInputStream(objectBytes),
                                             om);
                                promise.complete(name);
                            } finally {
                                entries.close();
                                data.close();
                            }
                        } catch (IOException ioe) {
                            promise.completeExceptionally(ioe);
                        }
                    });
        } catch (IOException ioe) {
            promise.completeExceptionally(ioe);
        }
        return promise;
    }

    public CompletableFuture<ReadHandle> openOffloadedLedger(String name) {
        CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
        try {
            S3Object object = s3.getObject(BUCKET_NAME, name);
            DataInputStream dis = new DataInputStream(object.getObjectContent());
            long ledgerId = dis.readLong();

            LedgerMetadataFormat.Builder builder = LedgerMetadataFormat.newBuilder();
            builder.mergeDelimitedFrom(dis);
            LedgerMetadataFormat metadata = builder.build();

            long numEntries = dis.readLong();
            List<LedgerEntryImpl> entries = new ArrayList<LedgerEntryImpl>();
            for (long id = 0; id < numEntries; id++) {
                long length = dis.readLong();
                byte[] bytes = new byte[(int)length];
                dis.read(bytes);
                entries.add(LedgerEntryImpl.create(ledgerId, id, length, Unpooled.wrappedBuffer(bytes)));
            }

            promise.complete(new ReadHandle() {
                    @Override
                    public long getId() {
                        return ledgerId;
                    }

                    @Override
                    public LedgerMetadata getLedgerMetadata() {
                        return new LedgerMetadata() {
                            @Override
                            public int getEnsembleSize() {
                                return metadata.getEnsembleSize();
                            }

                            @Override
                            public int getWriteQuorumSize() {
                                return metadata.getQuorumSize();
                            }

                            @Override
                            public int getAckQuorumSize() {
                                return metadata.getAckQuorumSize();
                            }

                            @Override
                            public long getLastEntryId() {
                                return metadata.getLastEntryId();
                            }

                            @Override
                            public long getLength() {
                                return metadata.getLength();
                            }

                            @Override
                            public DigestType getDigestType() {
                                return DigestType.CRC32;
                            }

                            @Override
                            public long getCtime() {
                                return metadata.getCtime();
                            }

                            @Override
                            public boolean isClosed() {
                                return metadata.getState() == LedgerMetadataFormat.State.CLOSED;
                            }

                            @Override
                            public Map<String, byte[]> getCustomMetadata() {
                                return null; // not needed for prototype
                            }

                            @Override
                            public List<BookieSocketAddress> getEnsembleAt(long entryId) {
                                return null; // not needed for prototype
                            }

                            @Override
                            public NavigableMap<Long, ? extends List<BookieSocketAddress>> getAllEnsembles() {
                                return null;
                            }
                        };
                    }

                    @Override
                    public CompletableFuture<Void> asyncClose() {
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public CompletableFuture<LedgerEntries> read(long firstEntry, long lastEntry) {
                        List<LedgerEntry> toReturn = new ArrayList<>();
                        for (long entryId = firstEntry; entryId <= lastEntry; entryId++) {
                            toReturn.add(entries.get((int)entryId).duplicate());
                        }
                        return CompletableFuture.completedFuture(LedgerEntriesImpl.create(toReturn));
                    }

                    @Override
                    public CompletableFuture<LedgerEntries> readUnconfirmed(long firstEntry, long lastEntry) {
                        return read(firstEntry, lastEntry);
                    }

                    @Override
                    public CompletableFuture<Long> readLastAddConfirmed() {
                        return CompletableFuture.completedFuture(getLastAddConfirmed());
                    }

                    @Override
                    public CompletableFuture<Long> tryReadLastAddConfirmed() {
                        return readLastAddConfirmed();
                    }

                    @Override
                    public long getLastAddConfirmed() {
                        return entries.size() - 1;
                    }

                    @Override
                    public long getLength() {
                        return getLedgerMetadata().getLength();
                    }

                    @Override
                    public boolean isClosed() {
                        return getLedgerMetadata().isClosed();
                    }

                    @Override
                    public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntry(long entryId,
                                                                                                 long timeOutInMillis,
                                                                                                 boolean parallel) {
                        return null; // not needed for prototype
                    }
                });
        } catch (Exception e) {
            promise.completeExceptionally(e);
        }
        return promise;
    }

}
