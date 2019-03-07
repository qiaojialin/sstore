import com.samsung.sra.datastore.*;
import com.samsung.sra.datastore.aggregates.SimpleCountOperator;
import com.samsung.sra.datastore.ingest.CountBasedWBMH;
import com.samsung.sra.datastore.storage.BackingStore;
import com.samsung.sra.datastore.storage.BackingStoreException;
import com.samsung.sra.datastore.storage.RocksDBBackingStore;

import java.io.File;
import java.io.IOException;

public class InsertTest {

    public static void main(String[] args) throws BackingStoreException, IOException, StreamException {

        String directory = "sstore";
        BackingStore backingStore = initBackingStore(directory);

        long streamId = 1;

        Stream stream = initStream(backingStore, streamId);

        for(long i = 1; i < 100; i++) {
            stream.append(i, i);
            if(i % 10000 == 0) {
                flush(stream);
            }
        }

        close(directory, backingStore, stream, streamId);

    }


    private static BackingStore initBackingStore(String directory) throws BackingStoreException {
        File dir = new File(directory);
        if (!dir.exists()) {
            boolean created = dir.mkdirs();
            assert created;
        }
        return new RocksDBBackingStore(directory + "/rocksdb", 0, false);

    }

    private static Stream initStream(BackingStore backingStore, long streamId) {
        // WBMH
        Windowing windowing = new GenericWindowing(new ExponentialWindowLengths(2));
        CountBasedWBMH wbmh = new CountBasedWBMH(windowing).setBufferSize(62);

        WindowOperator[] windowOperators = new WindowOperator[]{new SimpleCountOperator()};
        Stream stream = new Stream(streamId, wbmh, windowOperators);

        stream.populateTransientFields(backingStore);
        return stream;
    }


    private static void flush(Stream stream) throws BackingStoreException {
        stream.flush();
    }


    private static void close(String directory, BackingStore backingStore, Stream stream, long streamId) throws BackingStoreException, IOException {
        stream.flush();
        stream.unload(directory);
        stream.close();
        backingStore.putAux(streamId+"", Utilities.serialize(stream));
        stream.unload(directory);
        backingStore.close();
    }



}
