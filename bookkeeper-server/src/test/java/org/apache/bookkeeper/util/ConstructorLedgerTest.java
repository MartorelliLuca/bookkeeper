package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.PrimitiveIterator;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.*;

class ConstructorLedgerTest {

    // Helper method to convert long array to PrimitiveIterator.OfLong
    private PrimitiveIterator.OfLong getIterator(long[] array) {
        return LongStream.of(array).iterator();
    }

    @Test
    void testValidArray() {
        long[] validArray = {4, 5, 6};
        AvailabilityOfEntriesOfLedger ledger = new AvailabilityOfEntriesOfLedger(validArray);
        assertEquals(3, ledger.getTotalNumOfAvailableEntries());
        assertTrue(ledger.isEntryAvailable(4));
        assertTrue(ledger.isEntryAvailable(5));
        assertTrue(ledger.isEntryAvailable(6));
    }

    @Test
    void testEmptyArray() {
        long[] emptyArray = {};
        AvailabilityOfEntriesOfLedger ledger = new AvailabilityOfEntriesOfLedger(emptyArray);
        assertEquals(0, ledger.getTotalNumOfAvailableEntries());
    }

    @Test
    void testNullArray() {
        assertThrows(NullPointerException.class, () -> {
            new AvailabilityOfEntriesOfLedger((long[]) null);
        });
    }

    /*@Test
    void testNegativeArray() {
        long[] negativeArray = {-3, -2, -1};
        AvailabilityOfEntriesOfLedger ledger = new AvailabilityOfEntriesOfLedger(negativeArray);
        // Assuming negative values are ignored
        assertEquals(0, ledger.getTotalNumOfAvailableEntries());
    }*/

    @Test
    void testValidIterator() {
        long[] validArray = {4, 5, 6};
        PrimitiveIterator.OfLong iterator = getIterator(validArray);
        AvailabilityOfEntriesOfLedger ledger = new AvailabilityOfEntriesOfLedger(iterator);
        assertEquals(3, ledger.getTotalNumOfAvailableEntries());
        assertTrue(ledger.isEntryAvailable(4));
        assertTrue(ledger.isEntryAvailable(5));
        assertTrue(ledger.isEntryAvailable(6));
    }

    @Test
    void testEmptyIterator() {
        long[] emptyArray = {};
        PrimitiveIterator.OfLong iterator = getIterator(emptyArray);
        AvailabilityOfEntriesOfLedger ledger = new AvailabilityOfEntriesOfLedger(iterator);
        assertEquals(0, ledger.getTotalNumOfAvailableEntries());
    }

    @Test
    void testNullIterator() {
        assertThrows(NullPointerException.class, () -> {
            new AvailabilityOfEntriesOfLedger((PrimitiveIterator.OfLong) null);
        });
    }

    /*@Test
    void testNegativeIterator() {
        long[] negativeArray = {-3, -2, -1};
        PrimitiveIterator.OfLong iterator = getIterator(negativeArray);
        AvailabilityOfEntriesOfLedger ledger = new AvailabilityOfEntriesOfLedger(iterator);
        // Assuming negative values are ignored
        assertEquals(0, ledger.getTotalNumOfAvailableEntries());
    }*/

    @Test
    void testSerializedStateOfEntriesOfLedger() {
        // Create a valid ledger instance
        long[] validArray = {4, 5, 6};
        AvailabilityOfEntriesOfLedger ledger = new AvailabilityOfEntriesOfLedger(validArray);
        byte[] serializedState = ledger.serializeStateOfEntriesOfLedger();

        // Deserialize the state
        AvailabilityOfEntriesOfLedger deserializedLedger = new AvailabilityOfEntriesOfLedger(serializedState);
        assertEquals(3, deserializedLedger.getTotalNumOfAvailableEntries());
        assertTrue(deserializedLedger.isEntryAvailable(4));
        assertTrue(deserializedLedger.isEntryAvailable(5));
        assertTrue(deserializedLedger.isEntryAvailable(6));
    }

    @Test
    void testInvalidSerializedStateOfEntriesOfLedger() {
        byte[] invalidSerializedState = new byte[0];
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> {
            new AvailabilityOfEntriesOfLedger(invalidSerializedState);
        });
    }

    @Test
    void testByteBufSerializedStateOfEntriesOfLedger() {
        // Create a valid ledger instance
        long[] validArray = {4, 5, 6};
        AvailabilityOfEntriesOfLedger ledger = new AvailabilityOfEntriesOfLedger(validArray);
        byte[] serializedState = ledger.serializeStateOfEntriesOfLedger();
        ByteBuf byteBuf = Unpooled.wrappedBuffer(serializedState);

        // Deserialize the state
        AvailabilityOfEntriesOfLedger deserializedLedger = new AvailabilityOfEntriesOfLedger(byteBuf);
        assertEquals(3, deserializedLedger.getTotalNumOfAvailableEntries());
        assertTrue(deserializedLedger.isEntryAvailable(4));
        assertTrue(deserializedLedger.isEntryAvailable(5));
        assertTrue(deserializedLedger.isEntryAvailable(6));
    }

    @Test
    void testInvalidByteBufSerializedStateOfEntriesOfLedger() {
        ByteBuf invalidByteBuf = Unpooled.buffer(0);
        assertThrows(IndexOutOfBoundsException.class, () -> {
            new AvailabilityOfEntriesOfLedger(invalidByteBuf);
        });
    }
}
