package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class WriteCacheForEachTest {

    private class NullConsumer implements WriteCache.EntryConsumer{
        @Override
        public void accept(long ledgerId, long entryId, ByteBuf entry) throws IOException {
            throw new NullPointerException("Test Exception");
        }
    }

    @Test(expected = NullPointerException.class)
    public void testForEachWithNullConsumerPopulatedCache() throws IOException {
        WriteCache cache = new WriteCache(ByteBufAllocator.DEFAULT, 1024);
        ByteBuf entry = Unpooled.buffer();
        entry.writeBytes(new byte[]{1, 2, 3, 4});
        cache.put(1L, 1L, entry);
        cache.forEach(null);
    }

    @Test
    public void testForEachWithNullConsumerEmptyCache() throws IOException {
        WriteCache cache = new WriteCache(ByteBufAllocator.DEFAULT, 1024);
        cache.forEach(null);
    }


    @Test(expected = NullPointerException.class)
    public void testForEachWithInvalidConsumerPopulatedCache() throws IOException {
        WriteCache cache = new WriteCache(ByteBufAllocator.DEFAULT, 1024);
        NullConsumer consumer = new NullConsumer ();
        ByteBuf entry = Unpooled.buffer();
        entry.writeBytes(new byte[]{1, 2, 3, 4});
        cache.put(1L, 1L, entry);
        cache.forEach(consumer);
    }

    @Test
    public void testForEachWithInvalidConsumerEmptyCache() throws IOException {
        WriteCache cache = new WriteCache(ByteBufAllocator.DEFAULT, 1024);
        NullConsumer consumer = new NullConsumer ();
        cache.forEach(consumer);
    }

    @Test
    public void testForEachWithValidConsumerEmptyCache() throws IOException {
        WriteCache cache = new WriteCache(ByteBufAllocator.DEFAULT, 1024);
        List<String> result = new ArrayList<>();

        WriteCache.EntryConsumer consumer = new WriteCache.EntryConsumer() {
            @Override
            public void accept(long ledgerId, long entryId, ByteBuf entry) throws IOException {
                result.add(ledgerId + ":" + entryId);
            }
        };

        cache.forEach(consumer);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testForEachWithValidConsumerPopulatedCache() throws IOException {
        WriteCache cache = new WriteCache(ByteBufAllocator.DEFAULT, 1024);
        List<String> result = new ArrayList<>();

        WriteCache.EntryConsumer consumer = new WriteCache.EntryConsumer() {
            @Override
            public void accept(long ledgerId, long entryId, ByteBuf entry) throws IOException {
                result.add(ledgerId + ":" + entryId);
            }
        };
        ByteBuf entry = Unpooled.buffer();
        entry.writeBytes(new byte[]{1, 2, 3, 4});
        cache.put(1L, 1L, entry);

        cache.forEach(consumer);

        Assert.assertEquals(1, result.size());
        Assert.assertEquals("1:1", result.get(0));
    }


}
