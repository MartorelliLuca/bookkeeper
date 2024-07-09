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
    private final CacheState cacheState;
    private final boolean isExpectedException;
    private ByteBuf entry;

    private enum ByteBufType {
        NULL,
        VALID,
        EMPTY,
        INVALID
    }

    private enum CacheState {
        FULL,
        HALFFULL,
        EMPTY
    }

    @Parameterized.Parameters
    public static Collection<?> getParameters() {
        return Arrays.asList(new Object[][] {
                // ledgerID, entryID, entryType, cacheState, exception
                {-1, -1, ByteBufType.INVALID, CacheState.FULL, true}, // case 1
                {-1,  0, ByteBufType.INVALID, CacheState.FULL, true}, // case 2
                {0,   0, ByteBufType.INVALID, CacheState.FULL, true}, // case 3
                {0,  -1, ByteBufType.INVALID, CacheState.FULL, true}, // case 4
                {-1, -1, ByteBufType.VALID,   CacheState.FULL, true}, // case 5
                {-1,  0, ByteBufType.VALID,   CacheState.FULL, true}, // case 6
                {0,   0, ByteBufType.VALID,   CacheState.FULL, true}, // case 7
                {0,  -1, ByteBufType.VALID,   CacheState.FULL, true}, // case 8
                {-1, -1, ByteBufType.NULL,    CacheState.FULL, true}, // case 9
                {-1,  0, ByteBufType.NULL,    CacheState.FULL, true}, // case 10
                {0,   0, ByteBufType.NULL,    CacheState.FULL, true}, // case 11
                {0,  -1, ByteBufType.NULL,    CacheState.FULL, true}, // case 12
                {-1, -1, ByteBufType.EMPTY,   CacheState.FULL, true}, // case 13
                {-1,  0, ByteBufType.EMPTY,   CacheState.FULL, true}, // case 14
                {0,   0, ByteBufType.EMPTY,   CacheState.FULL, false},// case 15
                {0,  -1, ByteBufType.EMPTY,   CacheState.FULL, false},// case 16

                {-1, -1, ByteBufType.INVALID, CacheState.HALFFULL, true}, // case 17
                {-1,  0, ByteBufType.INVALID, CacheState.HALFFULL, true}, // case 18
                {0,   0, ByteBufType.INVALID, CacheState.HALFFULL, true}, // case 19
                {0,  -1, ByteBufType.INVALID, CacheState.HALFFULL, true}, // case 20
                {-1, -1, ByteBufType.VALID,   CacheState.HALFFULL, true}, // case 21
                {-1,  0, ByteBufType.VALID,   CacheState.HALFFULL, true}, // case 22
                {0,   0, ByteBufType.VALID,   CacheState.HALFFULL, false},// case 23
                {0,  -1, ByteBufType.VALID,   CacheState.HALFFULL, false}, // case 24
                {-1, -1, ByteBufType.NULL,    CacheState.HALFFULL, true}, // case 25
                {-1,  0, ByteBufType.NULL,    CacheState.HALFFULL, true}, // case 26
                {0,   0, ByteBufType.NULL,    CacheState.HALFFULL, true}, // case 27
                {0,  -1, ByteBufType.NULL,    CacheState.HALFFULL, true}, // case 28
                {-1, -1, ByteBufType.EMPTY,   CacheState.HALFFULL, true}, // case 29
                {-1,  0, ByteBufType.EMPTY,   CacheState.HALFFULL, true}, // case 30
                {0,   0, ByteBufType.EMPTY,   CacheState.HALFFULL, false},// case 31
                {0,  -1, ByteBufType.EMPTY,   CacheState.HALFFULL, false}, // case 32

                {-1, -1, ByteBufType.INVALID, CacheState.EMPTY, true}, // case 33
                {-1,  0, ByteBufType.INVALID, CacheState.EMPTY, true}, // case 34
                {0,   0, ByteBufType.INVALID, CacheState.EMPTY, true}, // case 35
                {0,  -1, ByteBufType.INVALID, CacheState.EMPTY, true}, // case 36
                {-1, -1, ByteBufType.VALID,   CacheState.EMPTY, true}, // case 37
                {-1,  0, ByteBufType.VALID,   CacheState.EMPTY, true}, // case 38
                {0,   0, ByteBufType.VALID,   CacheState.EMPTY, false},// case 39
                {0,  -1, ByteBufType.VALID,   CacheState.EMPTY, true}, // case 40
                {-1, -1, ByteBufType.NULL,    CacheState.EMPTY, true}, // case 41
                {-1,  0, ByteBufType.NULL,    CacheState.EMPTY, true}, // case 42
                {0,   0, ByteBufType.NULL,    CacheState.EMPTY, true}, // case 43
                {0,  -1, ByteBufType.NULL,    CacheState.EMPTY, true}, // case 44
                {-1, -1, ByteBufType.EMPTY,   CacheState.EMPTY, true}, // case 45
                {-1,  0, ByteBufType.EMPTY,   CacheState.EMPTY, true}, // case 46
                {0,   0, ByteBufType.EMPTY,   CacheState.EMPTY, false},// case 47
                {0,  -1, ByteBufType.EMPTY,   CacheState.EMPTY, true}, // case 48
        });
    }

    public PutWriteCacheTest(long ledgerId, long entryId, ByteBufType entryType, CacheState cacheState, boolean isExpectedException) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.entryType = entryType;
        this.cacheState = cacheState;
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

        switch (cacheState) {
            case FULL:
                for (int i = 0; i < entryNumber; i++) {
                    ByteBuf buf = byteBufAllocator.buffer(entrySize);
                    buf.writeBytes(new byte[entrySize]);
                    writeCache.put(i, i, buf);
                    buf.release();
                }
                break;
            case HALFFULL:
                for (int i = 0; i < entryNumber / 2; i++) {
                    ByteBuf buf = byteBufAllocator.buffer(entrySize);
                    buf.writeBytes(new byte[entrySize]);
                    writeCache.put(i, i, buf);
                    buf.release();
                }
                break;
            case EMPTY:
                // Cache is already empty by default
                break;
        }
    }

    @Test
    public void testPut() {
        boolean putResult;
        try {
            putResult = writeCache.put(this.ledgerId, this.entryId, this.entry);
            Assert.assertEquals(!this.isExpectedException, putResult);
            Assert.assertEquals(this.entry, writeCache.get(this.ledgerId, this.entryId));
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
