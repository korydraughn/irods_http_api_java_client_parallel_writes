package com.irods.sandbox;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.codec.binary.Base64;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

public class IRODSHTTPParallelWriteTest {

    private static final String baseURL = "http://localhost:9000/irods-http-api/0.5.0";
    private static final String username = "<username>";
    private static final String password = "<password>";
    private static final String localFilePath = "/path/to/local/file";

    static HttpClient httpClient = null;
    static String bearerHeader = null;
    static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws IOException, InterruptedException {
        final var start = Instant.now();

        final var streamCount = 4;
        final var threadPool = Executors.newFixedThreadPool(streamCount);

        httpClient = HttpClient.newBuilder().build();

        final var bearerToken = authenticate(username, password);
        System.out.println("bearerToken = " + bearerToken);
        bearerHeader = "Bearer " + bearerToken;

        final var logicalPath = String.format("/tempZone/home/%s/100mb.bin.http", username);
        final var parallelWriteHandle = parallelWriteInit(bearerToken, streamCount, logicalPath);
        System.out.println("parallelWriteHandle = " + parallelWriteHandle);

        final var fileSize = Files.size(Paths.get(localFilePath));
        final var chunkSize = fileSize / streamCount;
        final var chunkSizeRemainder = fileSize % streamCount;
        System.out.println(String.format("chunk size = %d, chunk size remainder = %d", chunkSize, chunkSizeRemainder));

        final var futures = new ArrayList<Future<?>>();

        // Launch worker tasks for uploading file.
        for (int i = 0; i < streamCount; ++i) {
            final var input = new WriteInput();

            input.httpClient = HttpClient.newHttpClient();
            input.executor = threadPool;

            input.bearerToken = bearerToken;
            input.localFilePath = localFilePath;
            input.parallelWriteHandle = parallelWriteHandle;
            input.streamIndex = i;

            input.offset = i * chunkSize;
            input.chunkSize = chunkSize + (i == streamCount - 1 ? chunkSizeRemainder : 0);

            futures.add(writeChunk(input));
        }

        // Wait for all parallel write threads to finish.
        futures.forEach(f -> {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        parallelWriteShutdown(bearerToken, parallelWriteHandle);

        final var duration = Duration.between(start, Instant.now());
        System.out.println(String.format("time elapsed: %d s %d ms", duration.toSeconds(), duration.minusSeconds(duration.toSeconds()).toMillis()));

        threadPool.shutdown();
    }

    static String authenticate(String username, String password) throws IOException, InterruptedException {
        final var creds = Base64.encodeBase64URLSafeString(
                String.format("%s:%s", username, password).getBytes());

        final var request = HttpRequest.newBuilder()
            .version(Version.HTTP_1_1)
            .uri(URI.create(baseURL + "/authenticate"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .header("Authorization", "Basic " + creds)
            .build();

        return httpClient.send(request, BodyHandlers.ofString()).body();
    }

    static class IRODSResponse
    {
        @JsonProperty("status_code") int errorCode;
    }

    static class Response
    {
        @JsonProperty("irods_response") IRODSResponse iRODSResponse;
        @JsonProperty("parallel_write_handle") String parallelWriteHandle;
    }

    static String parallelWriteInit(String bearerToken, int streamCount, String path) throws IOException, InterruptedException {
        final var body = String.format(
                "op=parallel_write_init&lpath=%s&stream-count=%d", path, streamCount);

        final var request = HttpRequest.newBuilder()
            .version(Version.HTTP_1_1)
            .uri(URI.create(baseURL + "/data-objects"))
            .POST(BodyPublishers.ofString(body))
            .header("Authorization", bearerHeader)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .build();

        final var response = httpClient.send(request, BodyHandlers.ofString());
        return mapper.readValue(response.body(), Response.class).parallelWriteHandle;
    }

    static class WriteInput
    {
        HttpClient httpClient;
        String bearerToken;
        String localFilePath;
        String parallelWriteHandle;
        int streamIndex;
        long offset;
        long chunkSize;
        ExecutorService executor;
        ExecutorService httpClientExecutor;
    }

    static Future<?> writeChunk(WriteInput input) {
        return input.executor.submit(() -> {
            final var bufferSize = 4 * 1024 * 1024;
            final var bbuf = new byte[bufferSize];

            try (final var in = new FileInputStream(input.localFilePath)) {
                in.skipNBytes(input.offset);

                long remainingBytes = input.chunkSize;
                long bytesRead = 0;
                boolean setOffset = true;
                final var boundary = "------BOUNDARY------";
                final var mpb = new MultipartBuilder(boundary);

                do {
                    bytesRead = in.read(bbuf);
                    System.out.println("bytesRead = " + bytesRead);

                    if (bytesRead <= 0) {
                        break;
                    }

                    final var byteCount = Math.min(remainingBytes, bytesRead);

                    mpb.clear();

                    if (setOffset) {
                        setOffset = false;
                        mpb.addPart("op", "write");
                        mpb.addPart("parallel-write-handle", input.parallelWriteHandle);
                        mpb.addPart("stream-index", String.valueOf(input.streamIndex));
                        mpb.addPart("offset", String.valueOf(input.offset));
                        mpb.addPart("count", String.valueOf(byteCount));
                        mpb.addPart("bytes", bbuf, (int) byteCount);
                    }
                    else {
                        mpb.addPart("op", "write");
                        mpb.addPart("parallel-write-handle", input.parallelWriteHandle);
                        mpb.addPart("stream-index", String.valueOf(input.streamIndex));
                        mpb.addPart("count", String.valueOf(byteCount));
                        mpb.addPart("bytes", bbuf, (int) byteCount);
                    }

                    mpb.addBoundaryEndMarker();

                    final var request = HttpRequest.newBuilder()
                        .version(Version.HTTP_1_1)
                        .uri(URI.create(baseURL + "/data-objects"))
                        .header("Authorization", bearerHeader)
                        .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                        .POST(BodyPublishers.ofByteArrays(mpb.build()))
                        .build();

                    try {
                        final var response = input.httpClient.send(request, BodyHandlers.ofString());
                        System.out.println(String.format("thread=[%d], response=[%s]", Thread.currentThread().getId(), response.toString()));
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }

                    input.offset += bytesRead;
                    remainingBytes -= bytesRead;
                }
                while (remainingBytes > 0);
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        });
    }

    static void parallelWriteShutdown(String bearerToken, String parallelWriteHandle) throws IOException, InterruptedException {
        final var body = String.format(
                "op=parallel_write_shutdown&parallel-write-handle=%s", parallelWriteHandle);

        final var request = HttpRequest.newBuilder()
            .version(Version.HTTP_1_1)
            .uri(URI.create(baseURL + "/data-objects"))
            .POST(BodyPublishers.ofString(body))
            .header("Authorization", bearerHeader)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .build();

        httpClient.send(request, BodyHandlers.ofString());
    }

    static class MultipartBuilder
    {
        private String boundary_;
        private StringBuilder sb_;
        private List<byte[]> byteArrays_;

        MultipartBuilder(String boundary)
        {
            boundary_ = boundary;
            sb_ = new StringBuilder();
            byteArrays_ = new ArrayList<>();
        }

        void addPart(String paramName, String value)
        {
            sb_.setLength(0);

            sb_.append("--");
            sb_.append(boundary_);
            sb_.append("\r\n");

            sb_.append("Content-Disposition: form-data; name=");
            sb_.append(paramName);
            sb_.append("\r\n");
            sb_.append("Content-Type: application/octet-stream");
            sb_.append("\r\n");
            sb_.append("Content-Transfer-Encoding: binary");
            sb_.append("\r\n");
            sb_.append("\r\n");

            sb_.append(value);
            sb_.append("\r\n");

            byteArrays_.add(sb_.toString().getBytes(StandardCharsets.UTF_8));
        }

        void addPart(String paramName, byte[] bytes, int count)
        {
            sb_.setLength(0);

            sb_.append("--");
            sb_.append(boundary_);
            sb_.append("\r\n");

            sb_.append("Content-Disposition: form-data; name=");
            sb_.append(paramName);
            sb_.append("\r\n");
            sb_.append("Content-Type: application/octet-stream");
            sb_.append("\r\n");
            sb_.append("Content-Transfer-Encoding: binary");
            sb_.append("\r\n");
            sb_.append("Content-Length: ");
            sb_.append(count);
            sb_.append("\r\n");
            sb_.append("\r\n");

            byteArrays_.add(sb_.toString().getBytes(StandardCharsets.UTF_8));
            byteArrays_.add(Arrays.copyOfRange(bytes, 0, count));
            byteArrays_.add("\r\n".getBytes(StandardCharsets.UTF_8));
        }

        void addBoundaryEndMarker()
        {
            sb_.setLength(0);

            sb_.append("--");
            sb_.append(boundary_);
            sb_.append("--");
            sb_.append("\r\n");

            byteArrays_.add(sb_.toString().getBytes(StandardCharsets.UTF_8));
        }

        List<byte[]> build()
        {
            return Collections.unmodifiableList(byteArrays_);
        }

        long size()
        {
            long count = 0;

            for (var array : byteArrays_) {
                count += array.length;
            }

            return count;
        }

        void clear()
        {
            sb_.setLength(0);
            byteArrays_.clear();
        }
    }

}
