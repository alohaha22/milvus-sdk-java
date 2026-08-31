/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.bulkwriter.storage.client;

import com.sun.net.httpserver.HttpServer;
import io.milvus.bulkwriter.connect.S3ConnectParam;
import io.minio.BucketExistsArgs;
import io.minio.credentials.Credentials;
import io.minio.credentials.Provider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MinioStorageClientTest {

    private HttpServer server;
    private final AtomicReference<String> authorizationHeader = new AtomicReference<>();
    private final AtomicInteger requestCount = new AtomicInteger();

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requestCount.incrementAndGet();
            authorizationHeader.set(exchange.getRequestHeaders().getFirst("Authorization"));
            // no such bucket: drives bucketExists down the false path
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    private String endpoint() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Test
    void buildsClientWithStaticKeys() {
        S3ConnectParam param = S3ConnectParam.newBuilder()
                .withCloudName("aws")
                .withEndpoint(endpoint())
                .withBucketName("bucket")
                .withAccessKey("ak")
                .withSecretKey("sk")
                .withRegion("us-west-2")
                .build();
        assertNotNull(MinioStorageClient.getStorageClient(param));
    }

    @Test
    void buildsClientWithExternalCredentialsProvider() {
        Provider provider = () -> new Credentials("ak", "sk", "token", null);
        S3ConnectParam param = S3ConnectParam.newBuilder()
                .withCloudName("aws")
                .withEndpoint(endpoint())
                .withBucketName("bucket")
                .withRegion("us-west-2")
                .withCredentialsProvider(provider)
                .build();
        assertNotNull(MinioStorageClient.getStorageClient(param));
    }

    @Test
    void gcpBearerHeaderComesFromProviderSessionToken() throws Exception {
        AtomicInteger fetchCalls = new AtomicInteger();
        // GCP convention: the bearer token rides in Credentials.sessionToken
        Provider provider = () -> {
            fetchCalls.incrementAndGet();
            return new Credentials("unused", "unused", "fresh-token", null);
        };
        S3ConnectParam param = S3ConnectParam.newBuilder()
                .withCloudName("gcp")
                .withEndpoint(endpoint())
                .withBucketName("bucket")
                .withRegion("us-west1")
                .withCredentialsProvider(provider)
                .build();
        MinioStorageClient client = MinioStorageClient.getStorageClient(param);

        assertFalse(client.bucketExists(BucketExistsArgs.builder().bucket("bucket").build()).get());

        assertEquals("Bearer fresh-token", authorizationHeader.get());
        assertTrue(fetchCalls.get() >= 1);
    }

    @Test
    void gcpStaticSessionTokenStillWorks() throws Exception {
        S3ConnectParam param = S3ConnectParam.newBuilder()
                .withCloudName("gcp")
                .withEndpoint(endpoint())
                .withBucketName("bucket")
                .withAccessKey("ak")
                .withSecretKey("sk")
                .withSessionToken("static-token")
                .withRegion("us-west1")
                .build();
        MinioStorageClient client = MinioStorageClient.getStorageClient(param);

        assertFalse(client.bucketExists(BucketExistsArgs.builder().bucket("bucket").build()).get());

        assertEquals("Bearer static-token", authorizationHeader.get());
    }
}
