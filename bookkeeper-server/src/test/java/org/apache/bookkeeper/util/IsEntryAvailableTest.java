package org.apache.bookkeeper.util;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.PrimitiveIterator;

@RunWith(Parameterized.class)
public class IsEntryAvailableTest {

    private AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger;
    private long entryId;
    private boolean expectedResult;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {-1L, false},
                {1L, false},
                {2L, true},
                {7L, true},
                {8L, false}
        });
    }

    public IsEntryAvailableTest(long entryId, boolean expectedResult) {
        this.entryId = entryId;
        this.expectedResult = expectedResult;
        initializeAvailabilityOfEntriesOfLedger();
    }

    private void initializeAvailabilityOfEntriesOfLedger() {
        long[] content = {2L, 6L, 7L};
        PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(content).iterator();
        this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(primitiveIterator);
    }

    @Test
    public void testIsEntryAvailable() {
        boolean actual = availabilityOfEntriesOfLedger.isEntryAvailable(entryId);
        Assert.assertEquals(expectedResult, actual);
    }

    @Test(expected = IllegalStateException.class)
    public void testIsEntryAvailableBeforeClosing() throws Exception {
        // Create a new instance but do not close it
        long[] content = {2L, 6L, 7L};
        PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(content).iterator();
        AvailabilityOfEntriesOfLedger notClosedLedger = new AvailabilityOfEntriesOfLedger(primitiveIterator);

        // Use reflection to set availabilityOfEntriesOfLedgerClosed to false
        Field closedField = AvailabilityOfEntriesOfLedger.class.getDeclaredField("availabilityOfEntriesOfLedgerClosed");
        closedField.setAccessible(true);
        closedField.set(notClosedLedger, false);

        // Attempt to call isEntryAvailable should throw IllegalStateException
        notClosedLedger.isEntryAvailable(2L);
    }
}
