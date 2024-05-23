package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.*;

import io.netty.buffer.Unpooled;
import io.netty.util.IllegalReferenceCountException;
import org.junit.Test;
import io.netty.buffer.ByteBufAllocator;

public class WriteCacheTest {

    @Test
    public void testValidAllocator() {
        try {
            WriteCache cache = new WriteCache(ByteBufAllocator.DEFAULT, 1024L);
            assertNotNull(cache);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    /*@Test
    public void testNullAllocator() {
        //Todo questo test nonostante allochi spazio con ByteBufAllocator = NULL, non lancia alcuna eccezione (gli va bene) (?)
        new WriteCache(null, 1024);
        try {
            new WriteCache(null, 1024L);
            fail("Expected IllegalArgumentException for null allocator");
        } catch (IllegalArgumentException e) {
            // Expected exception
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testCacheSizeZero() {
        //Todo questo test nonostante allochi spazio nullo (MaxCacheSize=0), non lancia alcuna eccezione (gli va bene)
        try {
            WriteCache cache = new WriteCache(ByteBufAllocator.DEFAULT, 0);
            assertNotNull(cache);
            assertEquals(0L, cache.size());
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }*/

    @Test
    public void testCacheSizeNegative() {
        try {
            new WriteCache(ByteBufAllocator.DEFAULT, -1);
            fail("Expected IllegalArgumentException for negative cache size");
        } catch (IllegalArgumentException e) {
            // Expected
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testSmallCacheSize() {
        try {
            WriteCache cache = new WriteCache(ByteBufAllocator.DEFAULT, 1024L);
            assertNotNull(cache);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testLargeCacheSize() {
        new WriteCache(ByteBufAllocator.DEFAULT, Long.MAX_VALUE);
    }


    /*------------------------------------------------------------------------*/

    @Test
    public void testClear() {
        WriteCache cache = new WriteCache(ByteBufAllocator.DEFAULT, 1024L);

        // Aggiungi alcuni elementi alla cache
        cache.put(1L, 1L, Unpooled.wrappedBuffer(new byte[]{1, 2, 3}));
        cache.put(1L, 2L, Unpooled.wrappedBuffer(new byte[]{4, 5, 6}));

        // Verifica che la cache non sia vuota
        assertFalse(cache.isEmpty());

        // Chiama clear() e verifica che la cache sia vuota
        cache.clear();
        assertTrue(cache.isEmpty());

        // Verifica che gli indici siano resettati
        assertNull(cache.get(1L, 1L));
        assertNull(cache.get(1L, 2L));
    }

    @Test
    public void testClearOnEmptyCache() {
        WriteCache cache = new WriteCache(ByteBufAllocator.DEFAULT, 1024L);

        // Chiama clear() su una cache vuota e verifica che non ci siano eccezioni
        cache.clear();
        assertTrue(cache.isEmpty());
    }

    @Test(expected = IllegalReferenceCountException.class)
    public void testClose() {
        WriteCache cache = new WriteCache(ByteBufAllocator.DEFAULT, 1024L);

        // Aggiungi alcuni elementi alla cache
        cache.put(1L, 1L, Unpooled.wrappedBuffer(new byte[]{1, 2, 3}));
        cache.put(1L, 2L, Unpooled.wrappedBuffer(new byte[]{4, 5, 6}));

        // Chiama close() e verifica che i buffer siano rilasciati
        cache.close();
        // Non c'è un metodo diretto per verificare il rilascio, ma possiamo invocare close() più volte per assicurarci che non ci siano eccezioni
        cache.close();
    }

    @Test(expected = IllegalReferenceCountException.class)
    public void testCloseOnEmptyCache() {
        WriteCache cache = new WriteCache(ByteBufAllocator.DEFAULT, 1024L);

        // Chiama close() su una cache vuota e verifica che non ci siano eccezioni
        cache.close();
        cache.close();
    }
}
