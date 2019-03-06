import com.samsung.sra.datastore.*;
import com.samsung.sra.datastore.aggregates.SimpleCountOperator;
import com.samsung.sra.datastore.ingest.CountBasedWBMH;
import com.samsung.sra.datastore.storage.BackingStore;
import com.samsung.sra.datastore.storage.BackingStoreException;
import com.samsung.sra.datastore.storage.MainMemoryBackingStore;
import com.samsung.sra.datastore.storage.RocksDBBackingStore;

import java.io.File;
import java.io.IOException;

public class InsertTest {

    private static String directory = "sstore";
    private static Stream stream = null;
    private static BackingStore backingStore = null;
    private static long streamId = 1;

    public static void main(String[] args) throws BackingStoreException, IOException, StreamException {

        // WBMH
        Windowing windowing = new GenericWindowing(new ExponentialWindowLengths(2));
        CountBasedWBMH wbmh = new CountBasedWBMH(windowing).setBufferSize(62);

        backingStore = getBackingStore();

        WindowOperator[] windowOperators = new WindowOperator[]{new SimpleCountOperator()};
        stream = new Stream(streamId, false, wbmh, windowOperators, false);
        stream.populateTransientFields(backingStore);

        for(long i = 0; i < 1000; i++) {
            stream.append(i, i);
        }

        close();

    }


    public static BackingStore getBackingStore() throws BackingStoreException {
        if (directory != null) {
            File dir = new File(directory);
            if (!dir.exists()) {
                boolean created = dir.mkdirs();
                assert created;
            }
            return new RocksDBBackingStore(directory + "/rocksdb", 0, false);
        } else {
            return new MainMemoryBackingStore();
        }
    }


    private static void close() throws BackingStoreException, IOException {
        stream.flush();
        stream.unload(directory);
        stream.close();
        backingStore.putAux(streamId+"", Utilities.serialize(stream));
        stream.unload(directory);
        backingStore.close();
    }



}
