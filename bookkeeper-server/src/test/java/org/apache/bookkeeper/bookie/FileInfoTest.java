package org.apache.bookkeeper.bookie;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.*;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class FileInfoTest {

    private File tempFile;
    private final byte[] masterKey = "masterKey".getBytes(StandardCharsets.UTF_8);

    @BeforeEach
    public void setUp() throws IOException {
        tempFile = File.createTempFile("testfile", ".idx");
    }

    @AfterEach
    public void tearDown() {
        if (tempFile.exists()) {
            tempFile.delete();
        }
    }

    private void writeHeader(File file, int version, byte[] masterKey, int stateBits, byte[] explicitLac) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            ByteBuffer bb = ByteBuffer.allocate((int) FileInfo.START_OF_DATA);
            bb.putInt(FileInfo.SIGNATURE);
            bb.putInt(version);
            bb.putInt(masterKey.length);
            bb.put(masterKey);
            bb.putInt(stateBits);
            if (version >= FileInfo.V1) {
                if (explicitLac != null) {
                    bb.putInt(explicitLac.length);
                    bb.put(explicitLac);
                } else {
                    bb.putInt(0);
                }
            }
            bb.rewind();
            fos.write(bb.array());
        }
    }

    private void writeHeader1(File file, int version, byte[] masterKey, int stateBits, byte[] explicitLac) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            ByteBuffer bb = ByteBuffer.allocate((int) FileInfo.START_OF_DATA);
            bb.putInt(FileInfo.SIGNATURE);
            bb.putInt(version+10);
            bb.putInt(masterKey.length);
            bb.put(masterKey);
            bb.putInt(stateBits);
            if (version >= FileInfo.V1) {
                if (explicitLac != null) {
                    bb.putInt(explicitLac.length);
                    bb.put(explicitLac);
                } else {
                    bb.putInt(0);
                }
            }
            bb.rewind();
            fos.write(bb.array());
        }
    }

    @Test
    public void testReadHeaderValid() throws IOException {
        writeHeader(tempFile, FileInfo.CURRENT_HEADER_VERSION, masterKey, 0, null);

        FileInfo fileInfo = new FileInfo(tempFile, masterKey, FileInfo.CURRENT_HEADER_VERSION);
        fileInfo.readHeader();

        assertArrayEquals(masterKey, fileInfo.getMasterKey());
        assertFalse(fileInfo.isFenced());
    }

    @Test
    public void testReadHeaderInvalidSignature() throws IOException {
        try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            ByteBuffer bb = ByteBuffer.allocate((int) FileInfo.START_OF_DATA);
            bb.putInt(0x12345678);  // Invalid signature
            bb.putInt(FileInfo.CURRENT_HEADER_VERSION);
            bb.putInt(masterKey.length);
            bb.put(masterKey);
            bb.putInt(0);
            bb.rewind();
            fos.write(bb.array());
        }

        FileInfo fileInfo = new FileInfo(tempFile, masterKey, FileInfo.CURRENT_HEADER_VERSION);
        IOException exception = assertThrows(IOException.class, fileInfo::readHeader);
        assertTrue(exception.getMessage().contains("Missing ledger signature"));
    }

    @Test
    public void testReadHeaderInvalidVersion() throws IOException {
        writeHeader1(tempFile, FileInfo.CURRENT_HEADER_VERSION + 1, masterKey, 0, null);

        FileInfo fileInfo = new FileInfo(tempFile, masterKey, FileInfo.CURRENT_HEADER_VERSION);
        IOException exception = assertThrows(IOException.class, fileInfo::readHeader);
        assertTrue(exception.getMessage().contains("Incompatible ledger version"));
    }

    @Test
    public void testReadHeaderInvalidMasterKeyLength() throws IOException {
        try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            ByteBuffer bb = ByteBuffer.allocate((int) FileInfo.START_OF_DATA);
            bb.putInt(FileInfo.SIGNATURE);
            bb.putInt(FileInfo.CURRENT_HEADER_VERSION);
            bb.putInt(-1);  // Invalid master key length
            bb.putInt(0);
            bb.rewind();
            fos.write(bb.array());
        }

        FileInfo fileInfo = new FileInfo(tempFile, masterKey, FileInfo.CURRENT_HEADER_VERSION);
        IOException exception = assertThrows(IOException.class, fileInfo::readHeader);
        assertTrue(exception.getMessage().contains("Length -1 is invalid"));
    }

    @Test
    public void testReadHeaderWithExplicitLac() throws IOException {
        byte[] explicitLac = new byte[16];  // Example explicit LAC
        writeHeader(tempFile, FileInfo.V1, masterKey, 0, explicitLac);

        FileInfo fileInfo = new FileInfo(tempFile, masterKey, FileInfo.CURRENT_HEADER_VERSION);
        fileInfo.readHeader();

        ByteBuf explicitLacBuf = fileInfo.getExplicitLac();
        assertNotNull(explicitLacBuf);
        assertEquals(explicitLac.length, explicitLacBuf.capacity());
    }

    // Additional tests to improve coverage

    @Test
    public void testSetAndIsFenced() throws IOException {
        writeHeader(tempFile, FileInfo.CURRENT_HEADER_VERSION, masterKey, 0, null);

        FileInfo fileInfo = new FileInfo(tempFile, masterKey, FileInfo.CURRENT_HEADER_VERSION);
        fileInfo.readHeader();

        assertFalse(fileInfo.isFenced());
        fileInfo.setFenced();
        assertTrue(fileInfo.isFenced());
    }

    @Test
    public void testWriteAndReadData() throws IOException {
        writeHeader(tempFile, FileInfo.CURRENT_HEADER_VERSION, masterKey, 0, null);

        FileInfo fileInfo = new FileInfo(tempFile, masterKey, FileInfo.CURRENT_HEADER_VERSION);
        fileInfo.readHeader();

        ByteBuffer writeBuffer = ByteBuffer.allocate(128);
        writeBuffer.put("TestData".getBytes(StandardCharsets.UTF_8));
        writeBuffer.flip();

        fileInfo.write(new ByteBuffer[]{writeBuffer}, 0);

        ByteBuffer readBuffer = ByteBuffer.allocate(128);
        fileInfo.read(readBuffer, 0, true);
        readBuffer.flip();

        byte[] readData = new byte[readBuffer.remaining()];
        readBuffer.get(readData);

        assertArrayEquals("TestData".getBytes(StandardCharsets.UTF_8), readData);
    }

    @Test
    public void testMoveToNewLocation() throws IOException {
        writeHeader(tempFile, FileInfo.CURRENT_HEADER_VERSION, masterKey, 0, null);

        File newFile = File.createTempFile("newfile", ".idx");
        newFile.deleteOnExit();

        FileInfo fileInfo = new FileInfo(tempFile, masterKey, FileInfo.CURRENT_HEADER_VERSION);
        fileInfo.readHeader();
        fileInfo.moveToNewLocation(newFile, Long.MAX_VALUE);

        FileInfo newFileInfo = new FileInfo(newFile, masterKey, FileInfo.CURRENT_HEADER_VERSION);
        newFileInfo.readHeader();

        assertArrayEquals(masterKey, newFileInfo.getMasterKey());
        assertFalse(newFileInfo.isFenced());
    }
}
