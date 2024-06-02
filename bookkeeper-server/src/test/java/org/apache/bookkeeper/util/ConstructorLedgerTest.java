package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.*;
import java.util.stream.LongStream;

@RunWith(Parameterized.class)
public class ConstructorLedgerTest {

    private enum CONSTRUCTOR {
        LongArray,
        Iterator,
        ByteBuf,
        ByteArray
    }

    private AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger;
    private long[] entriesOfLedger;
    private ByteBuf byteBuf;
    private boolean isExpectedException;
    private CONSTRUCTOR typeOfConstructor;

    public ConstructorLedgerTest(
            long[] entriesOfLedger,
            CONSTRUCTOR typeOfConstructor,
            boolean isExpectedException) {
        this.entriesOfLedger = entriesOfLedger;
        this.typeOfConstructor = typeOfConstructor;
        this.isExpectedException = isExpectedException;
        createEntriesOfLedger();
    }

    private void createEntriesOfLedger() {
        switch (typeOfConstructor) {
            case LongArray:
                availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(entriesOfLedger);
                break;
            case Iterator:
                LongStream longStream = Arrays.stream(entriesOfLedger);
                PrimitiveIterator.OfLong iterator = longStream.iterator();
                availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(iterator);
                break;
            case ByteArray:
                if (entriesOfLedger != null) { // Check if the array is null
                    int totalLenght = 64 + entriesOfLedger.length * Long.BYTES;
                    byte[] bytes = new byte[totalLenght];
                    SecureRandom random = new SecureRandom();
                    random.nextBytes(bytes);
                    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, 64, this.entriesOfLedger.length * Long.BYTES);
                    availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(byteBuffer.array());
                }
                break;
            case ByteBuf:
                int totalLengthByteBuf = 64 + entriesOfLedger.length * Long.BYTES;
                ByteBuf byteBuf = Unpooled.buffer(totalLengthByteBuf);
                SecureRandom secureRandom = new SecureRandom();
                byte[] randomBytesByteBuf = new byte[64];
                secureRandom.nextBytes(randomBytesByteBuf);
                byteBuf.writeBytes(randomBytesByteBuf);
                for (long l : this.entriesOfLedger) {
                    byteBuf.writeLong(l);
                }
                availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(byteBuf);
                break;
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        long[] valid = new long[]{1L, 2L, 4L};
        long[] invalid = new long[]{-1L, 4L, 8L};
        long[] empty = new long[5];

        return Arrays.asList(new Object[][] {
                //{entriesOfLedger,     TYPE_OF_CONSTRUCTORS,           EXCEPTION}
                {valid,                 CONSTRUCTOR.LongArray,          false},         //case 1
                {invalid,               CONSTRUCTOR.LongArray,          true},          //case 2
                {empty,                 CONSTRUCTOR.LongArray,          true},          //case 3
/*                {null,                  CONSTRUCTOR.LongArray,          true},          //case 4*/
                {valid,                 CONSTRUCTOR.Iterator,           false},         //case 5
                {invalid,               CONSTRUCTOR.Iterator,           true},          //case 6
                {empty,                 CONSTRUCTOR.Iterator,           true},          //case 7
                /*{null,                  CONSTRUCTOR.Iterator,           true},          //case 8
                {valid,                 CONSTRUCTOR.ByteArray,          true},          //case 9
                {invalid,               CONSTRUCTOR.ByteArray,          true},          //case 10
                {empty,                 CONSTRUCTOR.ByteArray,          true},          //case 11
                {null,                  CONSTRUCTOR.ByteArray,          true},          //case 12
                {valid,                 CONSTRUCTOR.ByteBuf,            false},         //case 13
                {invalid,               CONSTRUCTOR.ByteBuf,            true},          //case 14
                {empty,                 CONSTRUCTOR.ByteBuf,            true},          //case 15
                {null,                  CONSTRUCTOR.ByteBuf,            true}           //case 16*/
        });
    }

    @Test
    public void constructorTest() {
        try {
            Assert.assertNotNull(availabilityOfEntriesOfLedger);
            Assert.assertEquals(count(entriesOfLedger), availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries());
            for (long entry : entriesOfLedger) {
                if (entry >= 0) {
                    Assert.assertTrue(availabilityOfEntriesOfLedger.isEntryAvailable(entry));
                }
            }
        } catch (Exception e) {
            Assert.assertTrue(isExpectedException);
        }
    }

    private int count(long[] content) {
        Set<Long> uniqueEntries = new HashSet<>();
        for (long entry : content) {
            if (entry >= 0) {
                uniqueEntries.add(entry);
            }
        }
        return uniqueEntries.size();
    }
}