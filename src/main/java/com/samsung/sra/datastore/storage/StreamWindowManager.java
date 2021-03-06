/*
* Copyright 2016 Samsung Research America. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.samsung.sra.datastore.storage;

import com.samsung.sra.datastore.SummaryWindow;
import com.samsung.sra.datastore.WindowOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.stream.Stream;

/**
 * Handles all operations on the windows in one stream, including creating/merging/inserting into window objects and
 * getting window objects into/out of the backing store. Acts as a proxy to BackingStore: most code outside this package
 * should talk to StreamWindowManager and not to BackingStore directly.
 */
public class StreamWindowManager implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(StreamWindowManager.class);

    private transient BackingStore backingStore;
    public final long streamID;

    // summary information container
    private final WindowOperator[] operators;
    private final SerDe serde;

    public StreamWindowManager(long streamID, WindowOperator[] operators) {
        this.streamID = streamID;
        this.operators = operators;
        this.serde = new SerDe(operators);
    }

    public void populateTransientFields(BackingStore backingStore) {
        this.backingStore = backingStore;
    }

    public SummaryWindow createEmptySummaryWindow(long ts, long te, long cs, long ce) {
        return new SummaryWindow(operators, ts, te, cs, ce);
    }

    public void insertIntoSummaryWindow(SummaryWindow window, long ts, Object value) {
        assert window.ts <= ts && (window.te == -1 || ts <= window.te)
                && operators.length == window.aggregates.length;
        for (int i = 0; i < operators.length; ++i) {
            window.aggregates[i] = operators[i].insert(window.aggregates[i], ts, value);
        }
    }

    /** Replace windows[0] with union(windows) */
    public void mergeSummaryWindows(SummaryWindow... windows) {
        if (windows.length == 0) {
            return;
        }
        windows[0].ce = windows[windows.length - 1].ce;
        windows[0].te = windows[windows.length - 1].te;

        for (int opNum = 0; opNum < operators.length; ++opNum) {
            final int i = opNum; // work around Java dumbness re stream.map arguments
            windows[0].aggregates[i] = operators[i].merge(Stream.of(windows).map(b -> b.aggregates[i]));
        }
    }

    public SummaryWindow getSummaryWindow(long swid) throws BackingStoreException {
        return backingStore.getSummaryWindow(streamID, swid, serde);
    }

    /** Get all summary windows overlapping [t0, t1] */
    public Stream<SummaryWindow> getSummaryWindowsOverlapping(long t0, long t1) throws BackingStoreException {
        return  backingStore.getSummaryWindowsOverlapping(streamID, t0, t1, serde);
    }

    public void deleteSummaryWindow(long swid) throws BackingStoreException {
        backingStore.deleteSummaryWindow(streamID, swid, serde);
    }

    public void putSummaryWindow(SummaryWindow window) throws BackingStoreException {
        backingStore.putSummaryWindow(streamID, window.ts, serde, window);
    }

    public void flushToDisk() throws BackingStoreException {
        backingStore.flushToDisk(streamID, serde);
    }
}
