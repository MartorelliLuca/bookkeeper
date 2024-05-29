package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.apache.bookkeeper.bookie.storage.ldb.util.InvalidByteBuf;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class PutWriteCacheTest {
    private WriteCache writeCache;
    private final ByteBufAllocator byteBufAllocator;
    private final InvalidByteBuf invalidByteBuf;

    private final long ledgerId;
    private final long entryId;
    private final ByteBufType entryType;
    private final boolean isExpectedException;
    private ByteBuf entry;

    private enum ByteBufType {
        NULL,
        VALID,
        EMPTY,
        INVALID
    }

    @Parameterized.Parameters
    public static Collection<?> getParameters() {
        return Arrays.asList(new Object[][] {
                // ledgerID, entryID, entryType, exception
                //category partition tests
                {-1, -1, ByteBufType.INVALID, true},
                {-1,  0, ByteBufType.INVALID, true},
                {0,   0, ByteBufType.INVALID, true},
                {0,  -1, ByteBufType.INVALID, true},
                {-1, -1, ByteBufType.VALID,   true},
                {-1,  0, ByteBufType.VALID,   true},
                {0,   0, ByteBufType.VALID,   false},
                {0,  -1, ByteBufType.VALID,   true},
                {-1, -1, ByteBufType.NULL,    true},
                {-1,  0, ByteBufType.NULL,    true},
                {0,   0, ByteBufType.NULL,    true},
                {0,  -1, ByteBufType.NULL,    true},
                {-1, -1, ByteBufType.EMPTY,   true},
                {-1,  0, ByteBufType.EMPTY,   true},
                {0,   0, ByteBufType.EMPTY,   false},
                {0,  -1, ByteBufType.EMPTY,   true},
        });
    }

    public PutWriteCacheTest(long ledgerId, long entryId, ByteBufType entryType, boolean isExpectedException) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.entryType = entryType;
        this.isExpectedException = isExpectedException;
        this.byteBufAllocator = UnpooledByteBufAllocator.DEFAULT;
        this.invalidByteBuf = new InvalidByteBuf();
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
            case INVALID:
                this.entry = this.invalidByteBuf;
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
