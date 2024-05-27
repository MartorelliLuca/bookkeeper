package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.*;
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

/*    @Test
    public void testNullAllocator() {
        try {
            new WriteCache(null, 1024L);
            fail("Expected IllegalArgumentException for null allocator");
        } catch (IllegalArgumentException e) {
            // Expected
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testCacheSizeZero() {
        try {
            new WriteCache(ByteBufAllocator.DEFAULT, 0L);
            fail("Expected ArrayIndexOutOfBoundsException for cache size zero");
        } catch (ArrayIndexOutOfBoundsException e) {
           // Expected
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }*/

@Test
    public void testCacheSizeNegative() {
        try {
            new WriteCache(ByteBufAllocator.DEFAULT, -1L);
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

    @Test
    public void testLargeCacheSize() {
        try {
            new WriteCache(ByteBufAllocator.DEFAULT, Long.MAX_VALUE);
            fail("Expected ArrayIndexOutOfBoundsException for cache size zero");
        } catch (ArrayIndexOutOfBoundsException e) {
            // Expected
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }
}
