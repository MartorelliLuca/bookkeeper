package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class WriteCachePutTest {
    private WriteCache writeCache;
    private final ByteBufAllocator byteBufAllocator;

    private final long ledgerId;
    private final long entryId;
    private final EntryType entryType;
    private final boolean isExpectedException;
    private ByteBuf entry;

    private enum EntryType {
        NULL,
        VALID,
        EMPTY
    }

    //todo understand how to create an invalid ByteBuf

    @Parameterized.Parameters
    public static Collection<?> getParameters() {
        return Arrays.asList(new Object[][] {
       // ledgerID, entryID, entryType, exception
                {-1, -1, EntryType.VALID, true},
                {-1,  0, EntryType.VALID, true},
                {0,   0, EntryType.VALID, false},
                {0,  -1, EntryType.VALID, true},
                {-1, -1, EntryType.NULL,  true},
                {-1,  0, EntryType.NULL,  true},
                {0,   0, EntryType.NULL,  true},
                {0,  -1, EntryType.NULL,  true},
                {-1, -1, EntryType.EMPTY, true},
                {-1,  0, EntryType.EMPTY, true},
                {0,   0, EntryType.EMPTY, false},
                {0,  -1, EntryType.EMPTY, true}
        });
    }

    public WriteCachePutTest(long ledgerId, long entryId, EntryType entryType, boolean isExpectedException) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.entryType = entryType;
        this.isExpectedException = isExpectedException;
        this.byteBufAllocator = UnpooledByteBufAllocator.DEFAULT;
    }

    @Before
    public void setUp() {
        int entrySize = 1024;
        int entryNumber = 10;
        this.writeCache = new WriteCache(byteBufAllocator, entrySize * entryNumber);

        switch (entryType) {
            case NULL:
                this.entry = null;
                break;
            case VALID:
                this.entry = byteBufAllocator.buffer(entrySize);
                this.entry.writeBytes("bytes into the entry".getBytes());
                break;
            case EMPTY:
                this.entry = byteBufAllocator.buffer(entrySize);
                break;
        }
    }

    @Test
    public void testPut() {
        boolean putResult;
        try {
            putResult = writeCache.put(this.ledgerId, this.entryId, this.entry);
            Assert.assertEquals(!this.isExpectedException, putResult);
        } catch (Exception e) {
            Assert.assertTrue("Caught exception", this.isExpectedException);
        }
    }

    @After
    public void tearDown() {
        if (writeCache != null) {
            writeCache.clear();
            writeCache.close();
        }
        if (entry != null) {
            entry.release();
        }
    }
}
