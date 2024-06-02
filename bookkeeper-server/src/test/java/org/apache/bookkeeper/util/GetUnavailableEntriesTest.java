/*
package org.apache.bookkeeper.util;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Field;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class GetUnavailableEntriesTest {
    private final AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger;    // Object under test
    private final long startEntry;    // ID of the entry from which to search
    private final long lastEntry;     // ID ot the latest entry
    private final BitSet bookieEntries;   // Entries in bookie
    private final List<Long> expectedResult;  // Expected result

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        long lastBookieEntry = 9L;
        List<Long> exception = Collections.singletonList(-1L);

        BitSet validBitSet = new BitSet((int) lastBookieEntry);
        long[] bookieEntries1 = {2L, 4L, 5L, 7L};
        long[] bookieEntries2 = {5L, 7L, 9L}; // Caso per cui un SequenceGroup ha un sequencePeriod costante
        BitSet emptyBitSet = new BitSet((int) lastBookieEntry);;
        for (int i = 1; i < lastBookieEntry; i += 2) {
            validBitSet.set(i);
        }

        List<Long> unavailableEntries1 = Arrays.asList(1L, 3L);
        // BitSet con valori impostati oltre il range [startEntryId, lastEntryId]

        BitSet validBitsetButExceeding = new BitSet((int) lastBookieEntry + 10);   // sono presenti tutte le entry
        validBitsetButExceeding.flip(0, (int) lastBookieEntry + 11);

        BitSet bitSetWithoutMatchingValues = new BitSet(17);
        bitSetWithoutMatchingValues.flip(1, 2);

        List<Long> unavailableEntries2 = new ArrayList<>();
        for (long i = -1; i <= lastBookieEntry; i++) {
            if (!Arrays.asList(2L, 4L, 5L, 7L).contains(i)) {
                unavailableEntries2.add(i);
            }
        }

        BitSet exceedingBitSet = new BitSet((int) lastBookieEntry + 10);
        exceedingBitSet.flip(0, (int) lastBookieEntry + 11);

        List<Long> unavailableEntriesAllTrue = new ArrayList<>();
        for (long i = 0; i <= lastBookieEntry; i++) {
            if (!Arrays.asList(2L, 4L, 5L, 7L).contains(i)) {
                unavailableEntriesAllTrue.add(i);
            }
        }

        BitSet availabilityBitSet = new BitSet();
        availabilityBitSet.set(0, 10, true);
        List<Long> unavailableEntriesAll = new ArrayList<>();
        for (long i = 0; i < 10; i++) {
            unavailableEntriesAll.add(i);
        }

        return Arrays.asList(new Object[][]{
                {0, lastBookieEntry, validBitSet, bookieEntries1, unavailableEntries1},
                {lastBookieEntry, lastBookieEntry, bitSetWithoutMatchingValues, bookieEntries1, new ArrayList<>()},
                {0, lastBookieEntry, null, bookieEntries1, exception},
                {-1, lastBookieEntry, validBitsetButExceeding, bookieEntries1, unavailableEntries2},
                {lastBookieEntry, lastBookieEntry - 1, validBitSet, bookieEntries1, new ArrayList<>()},

                //caso aggiunto per migliorare la coverage di AvailabilityOfEntriesOfLedger (EmptyBitSet e SequencePeriod)
                {0, lastBookieEntry, validBitSet, bookieEntries2, unavailableEntries1},
                {0, lastBookieEntry, emptyBitSet, bookieEntries2, new ArrayList<>()},
                //caso aggiunto per testare che un AvailabilityOfEntriesOfLedger vuoto ritorni tutte le entry come non disponibili
                {0, 9, availabilityBitSet, new long[]{}, unavailableEntriesAll}
        });
    }

    public GetUnavailableEntriesTest(long startEntryId, long lastEntryId, BitSet expectedEntries,
                                     long[] bookieEntries, List<Long> expectedResult) {
        this.startEntry = startEntryId;
        this.lastEntry = lastEntryId;
        this.expectedResult = new ArrayList<>(expectedResult);
        this.bookieEntries = expectedEntries;

        PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(bookieEntries).iterator();
        this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(primitiveIterator);
    }

    @Test
    public void testAvailabilityOfEntries() {
        List<Long> result;
        try {
            result = this.availabilityOfEntriesOfLedger.getUnavailableEntries(this.startEntry, this.lastEntry, this.bookieEntries);
        } catch (NullPointerException e) {
            result = new ArrayList<>();
            result.add(-1L);
        }

        System.out.println(result);
        assertEquals(this.expectedResult, result);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetUnavailableEntryBeforeClosing() throws Exception {

        long[] content = {2L, 4L, 5L, 7L};
        PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(content).iterator();
        AvailabilityOfEntriesOfLedger notClosedLedger = new AvailabilityOfEntriesOfLedger(primitiveIterator);

        Field closedField = AvailabilityOfEntriesOfLedger.class.getDeclaredField("availabilityOfEntriesOfLedgerClosed");
        closedField.setAccessible(true);
        closedField.set(notClosedLedger, false);

        notClosedLedger.getUnavailableEntries(startEntry, lastEntry, bookieEntries);
    }
}
*/
