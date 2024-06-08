package org.apache.bookkeeper.bookie.storage.ldb;


import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.locks.ReentrantLock;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class WriteCacheForEachMutationsTest {

    private WriteCache writeCache;

    @Before
    public void setUp() {
        writeCache = new WriteCache(ByteBufAllocator.DEFAULT, 1024);
    }

    @Test
    public void testUnlockMethodCall() throws IOException, NoSuchFieldException, IllegalAccessException {
        // Crea un mock di ReentrantLock
        ReentrantLock lockMock = Mockito.mock(ReentrantLock.class);
        // Assegna il mock di ReentrantLock al campo sortedEntriesLock utilizzando la reflection
        setSortedEntriesLock(writeCache, lockMock);
        writeCache.forEach(consumerMock);
        verify(lockMock).unlock();
    }

    private final WriteCache.EntryConsumer consumerMock = new WriteCache.EntryConsumer() {
        @Override
        public void accept(long ledgerId, long entryId, ByteBuf entry) {
            // Implementazione del mock di EntryConsumer per il test
        }
    };


    // Metodo di utility per la reflection
    private void setSortedEntriesLock(WriteCache writeCache, ReentrantLock lock) throws NoSuchFieldException, IllegalAccessException {
        Field field = WriteCache.class.getDeclaredField("sortedEntriesLock");
        field.setAccessible(true);
        field.set(writeCache, lock);
    }
}
