package org.apache.bookkeeper.util;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class PITMutationTest {

    @Test
    public void testDeserializationWithNonZeroSequencePeriod() {
        //Create a valid AvailabilityOfEntriesOfLedger instance
        long[] entries = new long[]{1, 2, 3, 10, 11, 12};
        AvailabilityOfEntriesOfLedger availability = new AvailabilityOfEntriesOfLedger(entries);

        //Serialize the state
        byte[] serializedState = availability.serializeStateOfEntriesOfLedger();

        //Find the start of the sequence group data in the serialized state
        int sequenceGroupStartIndex = AvailabilityOfEntriesOfLedger.HEADER_SIZE;

        //Modify the sequencePeriod in the serialized state to a non-zero value
        ByteBuffer buffer = ByteBuffer.wrap(serializedState);
        buffer.position(sequenceGroupStartIndex + 2 * Long.BYTES + Integer.BYTES);
        buffer.putInt(5);

        // Deserialize the modified serialized state
        AvailabilityOfEntriesOfLedger reconstructedAvailability = new AvailabilityOfEntriesOfLedger(serializedState);

        // Verify that the deserialized instance has the modified sequencePeriod
        // This can be indirectly verified by checking the availability of entries
        assertTrue(reconstructedAvailability.isEntryAvailable(1));
        assertTrue(reconstructedAvailability.isEntryAvailable(2));
        assertTrue(reconstructedAvailability.isEntryAvailable(3));
        assertTrue(reconstructedAvailability.isEntryAvailable(10));
        assertTrue(reconstructedAvailability.isEntryAvailable(11));
        assertTrue(reconstructedAvailability.isEntryAvailable(12));

        // Verify that the reconstructedAvailability's sequencePeriod is reflected correctly in behavior
        // Entry 4 should not be available because of the sequencePeriod of 5
        assertFalse(reconstructedAvailability.isEntryAvailable(4));

        // Verify the total number of available entries is correct
        assertEquals(availability.getTotalNumOfAvailableEntries(), reconstructedAvailability.getTotalNumOfAvailableEntries());
    }
}
