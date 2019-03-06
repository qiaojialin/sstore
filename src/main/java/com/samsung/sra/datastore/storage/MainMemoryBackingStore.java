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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

public class MainMemoryBackingStore extends BackingStore {

    /** Map streamID -> windowID -> window */
    private Map<Long, ConcurrentSkipListMap<Long, SummaryWindow>> summaryWindows = new ConcurrentHashMap<>();
    private Map<String, byte[]> auxData = new ConcurrentHashMap<>();

    @Override
    SummaryWindow getSummaryWindow(long streamID, long swid, SerDe serDe) {
        return summaryWindows.get(streamID).get(swid);
    }

    @Override
    Stream<SummaryWindow> getSummaryWindowsOverlapping(long streamID, long t0, long t1, SerDe serDe) throws BackingStoreException {
        ConcurrentSkipListMap<Long, SummaryWindow> windows = summaryWindows.get(streamID);
        if (windows == null || windows.isEmpty()) {
            return Stream.empty();
        }
        Long l = windows.floorKey(t0);
        Long r = windows.higherKey(t1);
        if (l == null) {
            l = windows.firstKey();
        }
        if (r == null) {
            r = windows.lastKey() + 1;
        }
        return windows.subMap(l, true, r, false).values().stream();
    }

    @Override
    void deleteSummaryWindow(long streamID, long swid, SerDe serDe) {
        summaryWindows.get(streamID).remove(swid);
    }

    @Override
    void putSummaryWindow(long streamID, long swid, SerDe serDe, SummaryWindow window) {
        ConcurrentSkipListMap<Long, SummaryWindow> stream = summaryWindows.get(streamID);
        if (stream == null) {
            summaryWindows.put(streamID, (stream = new ConcurrentSkipListMap<>()));
        }
        stream.put(swid, window);
    }

    @Override
    long getNumSummaryWindows(long streamID, SerDe serDe) throws BackingStoreException {
        return summaryWindows.get(streamID).size();
    }


    @Override
    public byte[] getAux(String key) throws BackingStoreException {
        return auxData.get(key);
    }

    @Override
    public void putAux(String key, byte[] value) throws BackingStoreException {
        auxData.put(key, value);
    }

    @Override
    public void close() {
        summaryWindows.clear();
    }
}
