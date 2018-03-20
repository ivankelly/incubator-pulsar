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

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testng.Assert;
import org.testng.annotations.Test;

public class S3OffloaderTest extends MockedBookKeeperTestCase {
    private static final Logger log = LoggerFactory.getLogger(S3OffloaderTest.class);

    @Test
    public void testOffload() throws Exception {
        LedgerHandle lh = bkc.createLedger(2, 2, 2,
                                           BookKeeper.DigestType.CRC32, "".getBytes());
        for (int i = 0; i < 100; i++) {
            lh.addEntry(("foobar"+i).getBytes());
        }
        lh.close();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        S3Offloader offloader = new S3Offloader(executor);

        ReadHandle readHandle = bkc.newOpenLedgerOp()
            .withRecovery(true)
            .withLedgerId(lh.getId())
            .withDigestType(DigestType.CRC32)
            .withPassword("".getBytes()).execute().get();
        String name = offloader.offload(readHandle).get();

        ReadHandle readBack = offloader.openOffloadedLedger(name).get();

        Assert.assertEquals(readBack.getId(), readHandle.getId());
        Assert.assertEquals(readBack.getLastAddConfirmed(), readHandle.getLastAddConfirmed());

        try (LedgerEntries readEntries = readHandle.read(0, readHandle.getLastAddConfirmed()).get();
             LedgerEntries readBackEntries = readBack.read(0, readBack.getLastAddConfirmed()).get()) {
            Iterator<LedgerEntry> readIter = readEntries.iterator();
            Iterator<LedgerEntry> readBackIter = readBackEntries.iterator();

            while (readIter.hasNext()) {
                Assert.assertTrue(readBackIter.hasNext());
                LedgerEntry readEntry = readIter.next();
                LedgerEntry readBackEntry = readBackIter.next();

                Assert.assertEquals(readEntry.getEntryBuffer(), readBackEntry.getEntryBuffer());
            }
            Assert.assertFalse(readBackIter.hasNext());
        }
    }
}
