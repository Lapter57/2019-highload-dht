package ru.mail.polis.service.shakhmin;

import com.google.common.base.Charsets;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.shakhmin.LSMDao;
import ru.mail.polis.service.shakhmin.topology.Topology;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

final class HttpService {

    private static final Logger log = LoggerFactory.getLogger(HttpService.class);
    private static final String PROXY_HEADER_NAME = "X-OK-Proxy";
    private static final String PROXY_HEADER_VALUE = "true";
    static final String PROXY_HEADER = PROXY_HEADER_NAME + ": " + PROXY_HEADER_VALUE;

    @NotNull private final LSMDao dao;
    @NotNull private final Topology<String> topology;
    @NotNull private final HttpClient httpClient;

    HttpService(@NotNull final Executor proxyWorkers,
                @NotNull final DAO dao,
                @NotNull final Topology<String> topology) {
        this.dao = (LSMDao) dao;
        this.topology = topology;
        this.httpClient = HttpClient.newBuilder()
                .executor(proxyWorkers)
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    @NotNull
    Response get(@NotNull final MetaRequest meta) {
        if (meta.proxied()) {
            try {
                return Value.transform(
                        Value.from(dao.getCell(ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)))),
                        true);
            } catch (IOException e) {
                return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
            }
        }

        final var replicas = topology.replicas(
                ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)), meta.getRf().getFrom());
        int acks = 0;
        final var values = new ArrayList<Value>();
        if (replicas.contains(topology.whoAmI())) {
            try {
                values.add(Value.from(
                        dao.getCell(ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)))));
                acks++;
            } catch (IOException e) {
                log.error("[{}] Can't get {}", topology.whoAmI(), meta.getId(), e);
            }
        }
        for (final var response : getResponsesFromReplicas(replicas, meta)) {
            try {
                values.add(Value.from(response));
                acks++;
            } catch (IllegalArgumentException e) {
                log.error("[{}] Bad response", topology.whoAmI(), e);
            }
        }
        if (acks >= meta.getRf().getAck()) {
            return Value.transform(Value.merge(values), false);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    @NotNull
    Response upsert(@NotNull final MetaRequest meta) {
        if (meta.proxied()) {
            try {
                dao.upsert(ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)), meta.getValue());
                return new Response(Response.CREATED, Response.EMPTY);
            } catch (NoSuchElementException e) {
                return new Response(Response.NOT_FOUND, Response.EMPTY);
            } catch (IOException e) {
                return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
            }
        }

        final var replicas = topology.replicas(
                ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)), meta.getRf().getFrom());

        int acks = 0;
        if (replicas.contains(topology.whoAmI())) {
            try {
                dao.upsert(ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)), meta.getValue());
                acks++;
            } catch (IOException e) {
                log.error("[{}] Can't upsert {}={}",
                        topology.whoAmI(), meta.getId(), meta.getValue(), e);
            }
        }
        for (final var response : getResponsesFromReplicas(replicas, meta)) {
            if (response.statusCode() == 201) {
                acks++;
            }
        }

        if (acks >= meta.getRf().getAck()) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    @NotNull
    Response delete(@NotNull final MetaRequest meta) {
        if (meta.proxied()) {
            try {
                dao.remove(ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)));
                return new Response(Response.ACCEPTED, Response.EMPTY);
            } catch (NoSuchElementException e) {
                return new Response(Response.NOT_FOUND, Response.EMPTY);
            } catch (IOException e) {
                return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
            }
        }

        final var replicas = topology.replicas(
                ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)), meta.getRf().getFrom());

        int acks = 0;
        if (replicas.contains(topology.whoAmI())) {
            try {
                dao.remove(ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)));
                acks++;
            } catch (IOException e) {
                log.error("[{}] Can't remove {}={}",
                        topology.whoAmI(), meta.getId(), meta.getValue(), e);
            }
        }

        for (final var response : getResponsesFromReplicas(replicas, meta)) {
            if (response.statusCode() == 202) {
                acks++;
            }
        }

        if (acks >= meta.getRf().getAck()) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    @NotNull
    private List<HttpResponse<byte[]>> getResponsesFromReplicas(@NotNull final List<String> replicas,
                                                                @NotNull final MetaRequest meta) {
        final int acks = replicas.remove(topology.whoAmI())
                ? meta.getRf().getAck() - 1
                : meta.getRf().getAck();
        if (acks > 0) {
            final var futures = new ArrayList<CompletableFuture<HttpResponse<byte[]>>>();
            for (final var node : replicas) {
                final var requestBuilder = HttpRequest.newBuilder(URI.create(node + meta.getRequest().getURI()))
                        .setHeader(PROXY_HEADER_NAME, PROXY_HEADER_VALUE);
                final var body = meta.getRequest().getBody();
                switch (meta.getMethod()) {
                    case GET:
                        requestBuilder.GET();
                        break;
                    case PUT:
                        requestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
                        break;
                    case DELETE:
                        requestBuilder.DELETE();
                        break;
                    case POST:
                        requestBuilder.POST(HttpRequest.BodyPublishers.ofByteArray(body));
                        break;
                    default:
                        throw new IllegalArgumentException("Service doesn't support method "
                                + meta.getMethod());
                }
                final var request = requestBuilder.build();
                futures.add(httpClient.sendAsync(
                        request,
                        HttpResponse.BodyHandlers.ofByteArray()));
            }
            return getFirstResponses(futures, acks).join();
        }
        return Collections.emptyList();
    }
    @SuppressWarnings("FutureReturnValueIgnored")
    private static <T> CompletableFuture<List<T>> getFirstResponses(@NotNull final List<CompletableFuture<T>> futures,
                                                                    final int acks) {
        if (futures.size() < acks) {
            throw new IllegalArgumentException("Number of expected responses = "
                    + futures.size() + " but acks = " + acks);
        }
        final int maxFails = futures.size() - acks;
        final var fails = new AtomicInteger(0);
        final var responses = new CopyOnWriteArrayList<T>();
        final var result = new CompletableFuture<List<T>>();

        final BiConsumer<T,Throwable> biConsumer = (value, failure) -> {
            log.info("{}", value);
            if ((failure != null || value == null) && fails.incrementAndGet() > maxFails) {
                result.complete(Collections.unmodifiableList(responses));
            } else if (!result.isDone()) {
                responses.add(value);
                if (responses.size() == acks) {
                    result.complete(Collections.unmodifiableList(responses));
                }
            }
        };
        for (final var future : futures){
            future.whenCompleteAsync(biConsumer)
                    .exceptionally(ex -> {
                        log.error("Failed to get response from nodes", ex);
                        return null;
                    });
        }
        return result;
    }
}
