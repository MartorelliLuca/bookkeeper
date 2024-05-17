import static org.junit.Assert.assertEquals;

import io.grpc.netty.shaded.io.netty.buffer.ByteBufAllocator;
import org.apache.bookkeeper.bookie.BufferedChannel;
import org.junit.Assert;
import org.junit.Test;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.Charset;

public class BufferedChannelTest {

    @Test
    public void simpleTest() throws IOException {
        int a=1;
        Assert.assertEquals(a, 1);
    }
}
