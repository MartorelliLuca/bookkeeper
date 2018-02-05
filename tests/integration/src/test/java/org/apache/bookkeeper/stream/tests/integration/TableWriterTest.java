/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.stream.tests.integration;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.net.URI;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.kv.PTable;
import org.apache.bookkeeper.api.kv.PTableWriter;
import org.apache.bookkeeper.api.kv.result.KeyValue;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.admin.StorageAdminClient;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.NamespaceProperties;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Integration test for table service using table writer.
 */
@Ignore
@Slf4j
public class TableWriterTest extends StorageServerTestBase {

    @Rule
    public final TestName testName = new TestName();

    private final String namespace = "test_namespace";
    private OrderedScheduler scheduler;
    private StorageAdminClient adminClient;
    private StorageClient storageClient;
    private URI defaultBackendUri;

    @Before
    @Override
    public void setUp() throws Exception {
        spec.serveReadOnlyTable(true);
        super.setUp();
    }

    @Override
    protected void doSetup() throws Exception {
        defaultBackendUri = URI.create("distributedlog://" + cluster.getZkServers() + "/stream/storage");
        scheduler = OrderedScheduler.newSchedulerBuilder()
            .name("table-client-test")
            .numThreads(1)
            .build();
        StorageClientSettings settings = StorageClientSettings.newBuilder()
            .addEndpoints(cluster.getRpcEndpoints().toArray(new Endpoint[cluster.getRpcEndpoints().size()]))
            .usePlaintext(true)
            .build();
        String namespace = "test_namespace";
        adminClient = StorageClientBuilder.newBuilder()
            .withSettings(settings)
            .buildAdmin();
        storageClient = StorageClientBuilder.newBuilder()
            .withSettings(settings)
            .withNamespace(namespace)
            .build();
    }

    @Override
    protected void doTeardown() throws Exception {
        if (null != adminClient) {
            adminClient.close();
        }
        if (null != storageClient) {
            storageClient.close();
        }
        if (null != scheduler) {
            scheduler.shutdown();
        }
    }

    private static ByteBuf getLKey(int i) {
        return Unpooled.wrappedBuffer(String.format("test-lkey-%06d", i).getBytes(UTF_8));
    }

    private static ByteBuf getValue(int i) {
        return Unpooled.wrappedBuffer(String.format("test-val-%06d", i).getBytes(UTF_8));
    }

    @Test
    public void testTableWriterAPI() throws Exception {
        // Create a namespace
        NamespaceConfiguration nsConf = NamespaceConfiguration.newBuilder()
            .setDefaultStreamConf(DEFAULT_STREAM_CONF)
            .build();
        NamespaceProperties nsProps = result(adminClient.createNamespace(namespace, nsConf));
        assertEquals(namespace, nsProps.getNamespaceName());
        assertEquals(nsConf.getDefaultStreamConf(), nsProps.getDefaultStreamConf());

        // Create a stream
        String streamName = testName.getMethodName() + "_stream";
        StreamConfiguration streamConf = StreamConfiguration.newBuilder(DEFAULT_STREAM_CONF)
            .setIsReadonly(true)
            .build();
        StreamProperties streamProps = result(
            adminClient.createStream(namespace, streamName, streamConf));
        assertEquals(streamName, streamProps.getStreamName());
        assertEquals(
            StreamConfiguration.newBuilder(streamConf)
                .setBackendServiceUrl(defaultBackendUri.toString())
                .build(),
            streamProps.getStreamConf());

        // Open the table writer
        PTableWriter<ByteBuf, ByteBuf> tableWriter = result(storageClient.openPTableWriter(streamName));
        PTable<ByteBuf, ByteBuf> table = result(storageClient.openPTable(streamName));

        byte[] pKey = "test-pkey".getBytes(UTF_8);
        ByteBuf pKeyBuf = Unpooled.wrappedBuffer(pKey);

        // write a few kvs
        final int numKvs = 10;
        for (int i = 0; i < numKvs; i++) {
            ByteBuf lKeyBuf = getLKey(i);
            ByteBuf valBuf = getValue(i);

            result(tableWriter.write(i, pKeyBuf, lKeyBuf, valBuf));
        }

        ByteBuf lastLKeyBuf = getLKey(numKvs - 1);
        ByteBuf valBuf;
        while ((valBuf = result(table.get(pKeyBuf, lastLKeyBuf))) == null) {
            log.info("Key({}, {}) doesn't exist",
                new String(ByteBufUtil.getBytes(pKeyBuf), UTF_8),
                new String(ByteBufUtil.getBytes(lastLKeyBuf), UTF_8));
            Thread.sleep(100);
        }
        if (null != valBuf) {
            valBuf.release();
        }

        // read the kvs
        ByteBuf lStartKey = getLKey(0);
        ByteBuf lEndKey = getLKey(numKvs - 1);
        List<KeyValue<ByteBuf, ByteBuf>> kvs = result(
            table.range(pKeyBuf, lStartKey, lEndKey));
        assertEquals(numKvs, kvs.size());
        int i = 0;
        for (KeyValue<ByteBuf, ByteBuf> kv : kvs) {
            assertEquals(getLKey(i), kv.key());
            assertEquals(getValue(i), kv.value());
            ++i;
            kv.close();
        }
        assertEquals(numKvs, i);

        // delete the few kvs
        for (i = 0; i < numKvs; i++) {
            ByteBuf lKeyBuf = getLKey(i);

            result(tableWriter.write(i, pKeyBuf, lKeyBuf, null));
        }

        lastLKeyBuf = getLKey(numKvs - 1);
        while ((valBuf = result(table.get(pKeyBuf, lastLKeyBuf))) != null) {
            valBuf.release();
            Thread.sleep(100);
        }

        // get the ranges again
        kvs = result(table.range(pKeyBuf, lStartKey, lEndKey));
        assertTrue(kvs.isEmpty());

        byte[] lIncrKey = "test-incr-lkey".getBytes(UTF_8);
        ByteBuf lIncrKeyBuf = Unpooled.wrappedBuffer(lIncrKey);

        for (int j = 0; j < 5; j++) {
            result(tableWriter.increment(j, pKeyBuf, lIncrKeyBuf, 100L));
            Long number = null;
            while (number == null || number.longValue() != 100L * (j + 1)) {
                Thread.sleep(100);
                number = result(table.getNumber(pKeyBuf, lIncrKeyBuf));
            }
            assertEquals(100L * (j + 1), number.longValue());
        }
    }
}
