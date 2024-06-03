/*
package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.PrimitiveIterator;

import static org.junit.Assert.assertNotEquals;

public class OtherTest {

    @Test
    public void testSerializeStateOfEntriesOfLedger() {
        // Creare un'istanza di AvailabilityOfEntriesOfLedger
        AvailabilityOfEntriesOfLedger availability = new AvailabilityOfEntriesOfLedger(new long[]{1, 2, 3});

        // Serializzare lo stato
        byte[] serializedState = availability.serializeStateOfEntriesOfLedger();

        // Modificare il byte array in modo che non sia tutto zero
        serializedState[0] = 1;

        // Ricostruire l'oggetto AvailabilityOfEntriesOfLedger dal byte array modificato
        AvailabilityOfEntriesOfLedger reconstructedAvailability = new AvailabilityOfEntriesOfLedger(serializedState);

        // Verificare che i due oggetti siano diversi
        assertNotEquals(availability, reconstructedAvailability);
    }
}
*/
