package ru.mail.polis.service.shakhmin;

import com.google.common.base.Charsets;
import one.nio.http.HttpSession;
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
import java.util.concurrent.TimeUnit;
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
                .version(HttpClient.Version.HTTP_2)
                .build();
    }

    void get(@NotNull final HttpSession session,
             @NotNull final MetaRequest meta) {
        if (meta.proxied()) {
            try {
                final var response = Value.transform(
                        Value.from(dao.getCell(ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)))),
                        true);
                sendResponse(session, response);
                return;
            } catch (IOException e) {
                sendResponse(session, new Response(Response.INTERNAL_ERROR, Response.EMPTY));
                return;
            }
        }

        final var replicas = topology.replicas(
                ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)), meta.getRf().getFrom());

        final var acks = new AtomicInteger(0);
        final var values = new ArrayList<Value>();
        if (replicas.contains(topology.whoAmI())) {
            try {
                values.add(Value.from(
                        dao.getCell(ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)))));
                acks.incrementAndGet();
            } catch (IOException e) {
                log.error("[{}] Can't get {}", topology.whoAmI(), meta.getId(), e);
            }
        }

        getResponsesFromReplicas(replicas, meta)
                .whenCompleteAsync((responses, failure) -> {
                    for (final var response : responses) {
                        values.add(Value.from(response));
                       acks.incrementAndGet();
                    }
                    if (acks.get() >= meta.getRf().getAck()) {
                        sendResponse(session, Value.transform(Value.merge(values), false));
                    } else {
                        sendResponse(session, new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                    }
                })
                .exceptionally(ex -> {
                    log.error("Failed to get response from node", ex);
                    return null;
                });
    }

    void upsert(@NotNull final HttpSession session,
                @NotNull final MetaRequest meta) {
        if (meta.proxied()) {
            try {
                dao.upsert(ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)), meta.getValue());
                sendResponse(session, new Response(Response.CREATED, Response.EMPTY));
                return;
            } catch (NoSuchElementException e) {
                sendResponse(session, new Response(Response.NOT_FOUND, Response.EMPTY));
                return;
            } catch (IOException e) {
                sendResponse(session, new Response(Response.INTERNAL_ERROR, Response.EMPTY));
                return;
            }
        }

        final var replicas = topology.replicas(
                ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)), meta.getRf().getFrom());

        final var acks = new AtomicInteger(0);
        if (replicas.contains(topology.whoAmI())) {
            try {
                dao.upsert(ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)), meta.getValue());
                acks.incrementAndGet();
            } catch (IOException e) {
                log.error("[{}] Can't upsert {}={}",
                        topology.whoAmI(), meta.getId(), meta.getValue(), e);
            }
        }

        getResponsesFromReplicas(replicas, meta)
                .whenCompleteAsync((responses, failure) -> {
                    for (final var response : responses) {
                        if (response.statusCode() == 201) {
                            acks.incrementAndGet();
                        }
                    }
                    if (acks.get() >= meta.getRf().getAck()) {
                        sendResponse(session, new Response(Response.CREATED, Response.EMPTY));
                    } else {
                        sendResponse(session, new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                    }
                })
                .exceptionally(ex -> {
                    log.error("Failed to get response from node", ex);
                    return null;
                });
    }

    void delete(@NotNull final HttpSession session,
                @NotNull final MetaRequest meta) {
        if (meta.proxied()) {
            try {
                dao.remove(ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)));
                sendResponse(session, new Response(Response.ACCEPTED, Response.EMPTY));
                return;
            } catch (NoSuchElementException e) {
                sendResponse(session, new Response(Response.NOT_FOUND, Response.EMPTY));
                return;
            } catch (IOException e) {
                sendResponse(session, new Response(Response.INTERNAL_ERROR, Response.EMPTY));
                return;
            }
        }

        final var replicas = topology.replicas(
                ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)), meta.getRf().getFrom());

        final var acks = new AtomicInteger(0);
        if (replicas.contains(topology.whoAmI())) {
            try {
                dao.remove(ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)));
                acks.incrementAndGet();
            } catch (IOException e) {
                log.error("[{}] Can't remove {}={}",
                        topology.whoAmI(), meta.getId(), meta.getValue(), e);
            }
        }

        getResponsesFromReplicas(replicas, meta)
                .whenCompleteAsync((responses, failure) -> {
                    for (final var response : responses) {
                        if (response.statusCode() == 202) {
                            acks.incrementAndGet();
                        }
                    }
                    if (acks.get() >= meta.getRf().getAck()) {
                        sendResponse(session, new Response(Response.ACCEPTED, Response.EMPTY));
                    } else {
                        sendResponse(session, new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                    }
                })
                .exceptionally(ex -> {
                    log.error("Failed to get responses", ex);
                    return null;
                });
    }

    @NotNull
    private CompletableFuture<List<HttpResponse<byte[]>>> getResponsesFromReplicas(
            @NotNull final List<String> replicas,
            @NotNull final MetaRequest meta) {
        final int acks = replicas.remove(topology.whoAmI())
                ? meta.getRf().getAck() - 1
                : meta.getRf().getAck();
        if (acks > 0) {
            final var futures = new ArrayList<CompletableFuture<HttpResponse<byte[]>>>();
            for (final var node : replicas) {
                final var requestBuilder = HttpRequest
                        .newBuilder(URI.create(node + meta.getRequest().getURI()))
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
                futures.add(httpClient.sendAsync(
                        requestBuilder.build(),
                        HttpResponse.BodyHandlers.ofByteArray()));
            }
            return getFirstResponses(futures, acks);
        }
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

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
            if ((failure != null || value == null) && fails.incrementAndGet() > maxFails) {
                result.complete(responses);
            } else if (!result.isDone() && value != null) {
                responses.add(value);
                if (responses.size() == acks) {
                    result.complete(responses);
                }
            }
        };
        for (final var future : futures) {
            future.orTimeout(1, TimeUnit.SECONDS)
                  .whenCompleteAsync(biConsumer)
                  .exceptionally(ex -> {
                      log.error("Failed to get response from node", ex);
                      return null;
                  });
        }
        return result;
    }

    static void sendResponse(@NotNull final HttpSession session,
                                    @NotNull final Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            try {
                session.sendError(Response.INTERNAL_ERROR, "Internal error");
            } catch (IOException ex) {
                log.error("Error with send response {}", ex.getMessage());
            }
        }
    }
}
