package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.*;

/** Here there are some tests added to increment jacoco coverage and for killing some mutation */

public class WriteCacheBoundaryConditionsTest {
    private WriteCache writeCache;
    private final ByteBufAllocator byteBufAllocator;

    private int size;
    private ByteBuf entry;


    @Before
    public void setUp() {
        int number = 2;
        this.size = 64;
        writeCache = new WriteCache(byteBufAllocator, size * number);
    }

    public WriteCacheBoundaryConditionsTest() {
        this.byteBufAllocator = UnpooledByteBufAllocator.DEFAULT;
        this.entry = this.byteBufAllocator.buffer(this.size);
        this.entry.writeBytes("RHEngb7GUMiIrVIRPi44gMjNsSgNO63L5bGy6oFNhBtKk0XDQ1YIIvEROrbUGD20".getBytes());

    }

    @Test
    public void PutUncommonOrderTest(){
        boolean putResult;

        this.writeCache.put(0,1,this.entry);
        putResult = this.writeCache.put(0,0,this.entry);

        System.out.println("Expected result: "+ true +"\t|\tResult: "+putResult);
        System.out.println("----------------------------------------");
        Assert.assertTrue(putResult);
    }
    @Test
    public void PutInCacheFullTest(){
        boolean result;
        this.writeCache.put(0,0,this.entry);
        this.writeCache.put(0,1,this.entry);
        result = this.writeCache.put(0,2,this.entry);
        System.out.println("Expected result: "+ false +"\t|\tResult: "+result);
        System.out.println("----------------------------------------");
        Assert.assertFalse(result);
    }

    @After
    public void tearDown() throws Exception {
        writeCache.clear();
        if(entry != null ) entry.release();
        writeCache.close();
    }
}
