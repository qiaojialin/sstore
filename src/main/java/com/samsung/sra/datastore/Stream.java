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
package com.samsung.sra.datastore;

import com.samsung.sra.datastore.ingest.CountBasedWBMH;
import com.samsung.sra.datastore.storage.BackingStore;
import com.samsung.sra.datastore.storage.BackingStoreException;
import com.samsung.sra.datastore.storage.StreamWindowManager;
import com.samsung.sra.protocol.OpTypeOuterClass.OpType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static com.samsung.sra.datastore.Utilities.deserializeFromFile;
import static com.samsung.sra.datastore.Utilities.serializeToFile;


/**
 * One Summary Store stream. This class has the outermost level of the logic for all major API calls.
 */
public class Stream implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(Stream.class);

    final long streamID;
    private final WindowOperator[] operators;
    private final Map<OpType, Integer> operatorIndexes = new HashMap<>();

    // transient resets it to false on SummaryStore reopen
    private volatile transient boolean loaded;

    final StreamStatistics stats;
    private long tLastAppend = -1;

    /**
     * Must be loaded to handle any reads/writes
     */
    transient StreamWindowManager windowManager;
    /**
     * Needed to handle writes, but can be unloaded in read-only mode. Maintains write indexes internally
     */
    transient CountBasedWBMH wbmh;

    public void populateTransientFields(BackingStore backingStore) {
        if (windowManager != null) {
            windowManager.populateTransientFields(backingStore);
        }
        if (wbmh != null) {
            wbmh.populateTransientFields(windowManager);
        }
    }

    public Stream(long streamID, CountBasedWBMH wbmh, WindowOperator[] operators) {
        this.streamID = streamID;
        this.operators = operators;
        for (int i = 0; i < operators.length; ++i) {
            OpType opType = operators[i].getOpType();
            if (operatorIndexes.containsKey(operators[i].getOpType())) {
                logger.warn("Stream has multiple operators of same type; getOperatorIndex will return the first");
            } else {
                operatorIndexes.put(opType, i);
            }
        }
        this.wbmh = wbmh;
        windowManager = new StreamWindowManager(streamID, operators);
        stats = new StreamStatistics();
        loaded = true;
    }


    public boolean isLoaded() {
        return loaded;
    }

    public void load(String directory, boolean readonly, BackingStore backingStore) throws IOException, ClassNotFoundException {
        if (directory == null) {
            return; // in-memory store, do nothing
        }
        if (loaded) {
            return;
        }
        windowManager = deserializeFromFile(directory + "/read-index." + streamID);
        wbmh = readonly ? null : deserializeFromFile(directory + "/write-index." + streamID);
        populateTransientFields(backingStore);
        loaded = true;
    }

    public void unload(String directory) throws IOException, BackingStoreException {
        if (directory == null) {
            return; // in-memory store, do nothing
        }
        if (!loaded) {
            return;
        }
        if (wbmh == null) {
            return; // in readonly mode, unload should do nothing
        }
        serializeToFile(directory + "/read-index." + streamID, windowManager);
        serializeToFile(directory + "/write-index." + streamID, wbmh);
        wbmh.close();
        windowManager.flushToDisk();
        windowManager = null;
        wbmh = null;
        loaded = false;
    }

    public void append(long ts, Object value) throws BackingStoreException, StreamException {
        if (ts <= tLastAppend) {
            throw new StreamException(String.format("out-of-order insert in stream %d: ts = %d", streamID, ts));
        }
        tLastAppend = ts;
        stats.append(ts, value);
        // insert into decayed window sequence
        wbmh.append(ts, value);
    }


    public Object query(int operatorNum, long t0, long t1, Object[] queryParams) throws BackingStoreException {
        long T0 = stats.getTimeRangeStart(), T1 = stats.getTimeRangeEnd();
        if (t0 > T1 || t1 < T0) { // [T0, T1] does not overlap [t0, t1]
            return operators[operatorNum].getEmptyQueryResult();
        } else {
            t0 = Math.max(t0, T0);
            t1 = Math.min(t1, T1);
        }

        java.util.stream.Stream summaryWindows = windowManager.getSummaryWindowsOverlapping(t0, t1);

        // function:  get value in summary window
        Function<SummaryWindow, Object> summaryRetriever = b -> b.aggregates[operatorNum];

        try {
            return operators[operatorNum].query(
                    stats, summaryWindows, summaryRetriever, t0, t1, queryParams);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof BackingStoreException) {
                throw (BackingStoreException) e.getCause();
            } else {
                throw e;
            }
        }
    }


    public void flush() throws BackingStoreException {
        wbmh.flush();
    }

    public void close() throws BackingStoreException {
        if (!loaded) {
            return;
        }
        if (wbmh == null) {
            return; // readonly mode
        }
        wbmh.close();
        windowManager.flushToDisk();
    }
}
