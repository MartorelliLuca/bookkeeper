package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;


@RunWith(Parameterized.class)
public class GetWriteCacheTest {

    private WriteCache writeCache;
    private ByteBufAllocator byteBufAllocator;
    private ByteBuf entry;
    private final long ledgerId = 2;
    private final long entryId = 1;
    private final boolean isValidLedgerId;
    private final boolean isValidEntryId;
    private final boolean isExceptionExpected;


    @Before
    public void setUp() throws Exception {
        byteBufAllocator = ByteBufAllocator.DEFAULT;
        int entryNumber = 10;
        int entrySize = 1024;
        writeCache = new WriteCache(byteBufAllocator, entrySize * entryNumber);

        entry = Unpooled.wrappedBuffer("bytes into the entry".getBytes());
        writeCache.put(ledgerId, entryId, entry);
    }

    @After
    public void tearDown() throws Exception {
        writeCache.clear();
        entry.release();
        writeCache.close();
    }

    // Input parameters
    @Parameterized.Parameters
    public static Collection<?> getParameters(){
        return Arrays.asList(new Object[][] {
                // isValidLedgerId, isValidEntryId, isExceptionExpected
                {  true,        true,           false},
                {  true,        false,          true},
                {  false,       true,           true},
                {  false,       false,          true}
        });
    }

    public GetWriteCacheTest(boolean isValidLedgerId, boolean isValidEntryId , boolean isExceptionExpected){
        this.isValidLedgerId = isValidLedgerId;
        this.isValidEntryId = isValidEntryId;
        this.isExceptionExpected = isExceptionExpected;
    }

    @Test
    public void getFromCacheTest(){

        ByteBuf result = null;

        long actualLedgerId = this.isValidLedgerId ? ledgerId : ledgerId + 1;
        long actualEntryId = this.isValidEntryId ? entryId : entryId + 1;

        try {
            System.out.println("Valid Ledger ID: " + this.ledgerId  + "\t|\t Actual Ledger ID: " + actualLedgerId);
            System.out.println("Valid Entry ID:  " + this.entryId  + "\t|\t Actual Entry ID:  " + actualEntryId);
            System.out.println("----------------------------------------");
            result = writeCache.get(actualLedgerId, actualEntryId);
        } catch(Exception e) {
            e.printStackTrace();
            Assert.assertTrue("Caught exception", this.isExceptionExpected);
        }

        if (!this.isExceptionExpected) {
            Assert.assertEquals(result, entry);
        } else {
            Assert.assertNull(result);
        }
    }
}
