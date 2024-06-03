package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.util.*;

@RunWith(Parameterized.class)
public class ConstructorLedgerTest {

    public enum CONSTRUCTOR {
        LongArray,
        Iterator,
        ByteArray,
        ByteBuf
    }

    private AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger;
    private PrimitiveIterator.OfLong entriesOfLongIterator;
    private long[] entriesOfLedger;
    private byte[] serializedEntries;
    private ByteBuf byteBuf;
    private final boolean isExpectedException;
    private final boolean isExpectedFailure;
    private final CONSTRUCTOR typeOfConstructor;

    public ConstructorLedgerTest(
            long[] entriesOfLedger,
            CONSTRUCTOR typeOfConstructor,
            boolean isExpectedException,
            boolean isExpectedFailure) {

        this.entriesOfLedger = entriesOfLedger;
        this.typeOfConstructor = typeOfConstructor;
        this.isExpectedException = isExpectedException;
        this.isExpectedFailure = isExpectedFailure;
        initializeEntries();
    }

    private void initializeEntries() {
        switch (typeOfConstructor) {
            case LongArray:
                break;
            case Iterator:
                if (entriesOfLedger != null) {
                    entriesOfLongIterator = Arrays.stream(entriesOfLedger).iterator();
                } else {
                    entriesOfLongIterator = null;
                }
                break;
            case ByteArray:
                if (entriesOfLedger != null) {
                    availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(entriesOfLedger);
                    serializedEntries = availabilityOfEntriesOfLedger.serializeStateOfEntriesOfLedger();
                } else {
                    serializedEntries = null;
                }
                break;
            case ByteBuf:
                if (entriesOfLedger != null) {
                    availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(entriesOfLedger);
                    byteBuf = Unpooled.buffer();
                    byteBuf.writeBytes(availabilityOfEntriesOfLedger.serializeStateOfEntriesOfLedger());
                } else {
                    byteBuf = null;
                }
                break;
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        long[] valid = new long[]{1L, 2L, 4L};
        long[] invalid = new long[]{-1L, 4L, 8L};
        long[] empty = new long[3];

        return Arrays.asList(new Object[][] {
              //{entriesOfLedger,        TYPE_OF_CONSTRUCTORS,           EXCEPTION,      FAILURE}
                {valid,                  CONSTRUCTOR.LongArray,          false,          false},
                {invalid,                CONSTRUCTOR.LongArray,          true,           true},
                {empty,                  CONSTRUCTOR.LongArray,          false,          false},
                {null,                   CONSTRUCTOR.LongArray,          true,           false},
                {valid,                  CONSTRUCTOR.Iterator,           false,          false},
                {invalid,                CONSTRUCTOR.Iterator,           true,           true},
                {empty,                  CONSTRUCTOR.Iterator,           false,          false},
                {null,                   CONSTRUCTOR.Iterator,           true,           false},
                {valid,                  CONSTRUCTOR.ByteArray,          false,          false},
                {invalid,                CONSTRUCTOR.ByteArray,          true,           true},
                {empty,                  CONSTRUCTOR.ByteArray,          false,          false},
                {null,                   CONSTRUCTOR.ByteArray,          true,           false},
                {valid,                  CONSTRUCTOR.ByteBuf,            false,          false},
                {invalid,                CONSTRUCTOR.ByteBuf,            true,           true},
                {empty,                  CONSTRUCTOR.ByteBuf,            false,          false},
                {null,                   CONSTRUCTOR.ByteBuf,            true,           false},

                {valid,                  CONSTRUCTOR.ByteArray,          true,           false}
        });
    }

    @Test
    public void constructorTest() {
        try {
            switch (typeOfConstructor) {
                case LongArray:
                    availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(entriesOfLedger);
                    break;
                case Iterator:
                    availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(entriesOfLongIterator);
                    break;
                case ByteArray:
                    if (serializedEntries != null) {
                        availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(serializedEntries);
                    } else {
                        throw new IllegalArgumentException("Serialized entries are null");
                    }
                    break;
                case ByteBuf:
                    if (byteBuf != null) {
                        availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(byteBuf);
                    } else {
                        throw new IllegalArgumentException("ByteBuf is null");
                    }
                    break;
            }

            if (!isExpectedFailure) {
                Assert.assertEquals(count(entriesOfLedger), availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries());
            }

            for (long entry : entriesOfLedger) {
                if (entry >= 0 && !isExpectedFailure) {
                    Assert.assertTrue(availabilityOfEntriesOfLedger.isEntryAvailable(entry));
                } else {
                    Assert.assertTrue(isExpectedFailure);
                }
            }
        } catch (Exception e) {
            Assert.assertTrue(isExpectedException);
        }
    }

    private int count(long[] content) {
        if (content == null) {
            return 0;
        }
        Set<Long> uniqueEntries = new HashSet<>();
        for (long entry : content) {
            if (entry >= 0) {
                uniqueEntries.add(entry);
            }
        }
        return uniqueEntries.size();
    }
}
