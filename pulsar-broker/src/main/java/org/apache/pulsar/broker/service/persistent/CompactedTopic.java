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
package org.apache.pulsar.broker.service.persistent;

import java.util.*;
import org.apache.pulsar.compactor.Compactor;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.bookkeeper.client.*;
import org.apache.pulsar.compactor.Compactor.CompactionSpec;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactedTopic {
    private static final Logger log = LoggerFactory.getLogger(CompactedTopic.class);

    private final BookKeeper bk;
    private CompactionSpec currentSpec;
    private LedgerHandle currentLedger;

    CompactedTopic(BookKeeper bk) {
        this.bk = bk;
    }

    synchronized void setSpec(CompactionSpec spec) {
        try {
            currentLedger = bk.openLedger(spec.ledgerId,
                                          BookKeeper.DigestType.CRC32,
                                          "".getBytes());
            currentSpec = spec;
        } catch (Exception e) {
            log.error("Couldn't update compaction spec", e);
        }
    }

    private long findStartPoint(LedgerHandle lh,
                                long start,
                                long end,
                                MessageIdImpl p) throws Exception {
        long midpoint = start + (end - start);
        LedgerEntry startEntry = lh.readEntries(start, start).nextElement();
        LedgerEntry midEntry = lh.readEntries(midpoint, midpoint).nextElement();
        LedgerEntry endEntry = lh.readEntries(end, end).nextElement();

        if (p.compareTo(Compactor.toMessageId(startEntry.getEntryBuffer())) < 0) {
            return start;
        } else if (p.compareTo(Compactor.toMessageId(midEntry.getEntryBuffer())) < 0) {
            return findStartPoint(lh, start, midpoint, p);
        } else if (p.compareTo(Compactor.toMessageId(endEntry.getEntryBuffer())) <= 0) {
            return findStartPoint(lh, midpoint, end, p);
        } else {
            return -1;
        }
    }

    public boolean asyncReadEntries(ManagedCursor cursor,
                                    int numberOfEntriesToRead,
                                    ReadEntriesCallback callback, Object ctx) {
        final LedgerHandle lh;
        final CompactionSpec spec;
        synchronized (this) {
            lh = currentLedger;
            spec = currentSpec;
        }
        PositionImpl position = (PositionImpl)cursor.getReadPosition();
        MessageIdImpl startCursor = new MessageIdImpl(position.getLedgerId(),
                                                      position.getEntryId(), -1);
        if (spec == null || startCursor.compareTo(spec.position) >= 0) {
            return false;
        }

        long entryId = -1;
        try {
            entryId = findStartPoint(lh, 0,
                                     lh.getLastAddConfirmed(), startCursor);
            log.info("IKDEBUG Start point found {}", entryId);
        } catch (Exception e) {
            log.error("Error finding start point", e);
        }

        if (entryId == -1) {
            return false;
        } else {
            long lastEntry = Math.min(lh.getLastAddConfirmed(),
                                      entryId + numberOfEntriesToRead);
            lh.asyncReadEntries(entryId, lastEntry,
                                (rc, _lh, seq, _ctx) -> {
                                    if (rc == 0) {
                                        try {
                                            List<Entry> entries = new ArrayList<>();
                                            MessageIdImpl lastId = null;
                                            while (seq.hasMoreElements()) {
                                                LedgerEntry e = seq.nextElement();
                                                lastId = Compactor.toMessageId(e.getEntryBuffer());
                                                entries.add(Compactor.toMLEntry(
                                                                    e.getEntryBuffer()));
                                            }
                                            cursor.seek(
                                                    new PositionImpl(
                                                            lastId.getLedgerId(),
                                                            lastId.getEntryId()
                                                                     ).getNext());
                                            callback.readEntriesComplete(
                                                    entries, ctx);
                                        } catch (Exception e) {
                                            callback.readEntriesFailed(
                                                    new ManagedLedgerException(e), ctx);
                                        }
                                    } else {
                                        callback.readEntriesFailed(
                                                new ManagedLedgerException(
                                                        BKException.create(rc)), ctx);
                                    }
                                }, null);
            return true;
        }
    }
}
