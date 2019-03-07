import com.samsung.sra.datastore.*;
import com.samsung.sra.datastore.storage.BackingStore;
import com.samsung.sra.datastore.storage.BackingStoreException;
import com.samsung.sra.datastore.storage.RocksDBBackingStore;

import java.io.File;
import java.io.IOException;

public class QueryTest {

    private static String directory = "sstore";
    private static Stream stream = null;
    private static BackingStore backingStore = null;
    private static long streamId = 1;


    public static void main(String[] args) throws BackingStoreException, IOException, ClassNotFoundException {

        backingStore = getBackingStore(directory);

        stream = Utilities.deserialize(backingStore.getAux(streamId + ""));
        stream.load(directory, true, backingStore);

        Object result = query(stream, 0, 10000, 0);

        System.out.println(result);

    }

    private static Object query(Stream stream, long t0, long t1, int aggregateNum, Object... queryParams)
            throws BackingStoreException {
        return stream.query(aggregateNum, t0, t1, queryParams);
    }


    private static BackingStore getBackingStore(String directory) throws BackingStoreException {
        File dir = new File(directory);
        if (!dir.exists()) {
            boolean created = dir.mkdirs();
            assert created;
        }
        return new RocksDBBackingStore(directory + "/rocksdb", 0, true);
    }
}
