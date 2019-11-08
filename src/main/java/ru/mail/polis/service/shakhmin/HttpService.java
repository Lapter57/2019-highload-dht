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
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static ru.mail.polis.service.shakhmin.FutureUtils.getResponsesFromReplicas;
import static ru.mail.polis.service.shakhmin.ResponseUtils.sendResponse;

final class HttpService {

    private static final Logger log = LoggerFactory.getLogger(HttpService.class);
    static final String PROXY_HEADER_NAME = "X-OK-Proxy";
    static final String PROXY_HEADER_VALUE = "true";
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
            CompletableFuture.runAsync(() -> {
                try {
                    final var response = Value.transform(
                            Value.from(dao.getCell(ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)))),
                            true);
                    sendResponse(session, response);
                } catch (IOException e) {
                    sendResponse(session, new Response(Response.INTERNAL_ERROR, Response.EMPTY));
                }
            })
            .exceptionally(ex -> {
                log.error("Failed to get from the dao", ex);
                return null;
            });
            return;
        }

        final var values = new ArrayList<Value>();
        final var replicas = topology.replicas(
                ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)), meta.getRf().getFrom());

        handleLocally(replicas, () -> getFromDao(meta.getId(), values))
                .thenComposeAsync(handled -> getResponsesFromReplicas(replicas, meta, topology, httpClient))
                .whenCompleteAsync((responses, failure) -> ResponseUtils.checkGetResponses(
                        replicas.contains(topology.whoAmI()) ? 1 : 0,
                        meta.getRf().getAck(),
                        values, responses, session
                ))
                .exceptionally(ex -> {
                    log.error(FutureUtils.FUTURE_ERROR_LOG, ex);
                    return null;
                });
    }

    void upsert(@NotNull final HttpSession session,
                @NotNull final MetaRequest meta) {
        if (meta.proxied()) {
            CompletableFuture.runAsync(() -> {
                try {
                    dao.upsert(ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)), meta.getValue());
                    sendResponse(session, new Response(Response.CREATED, Response.EMPTY));
                } catch (NoSuchElementException e) {
                    sendResponse(session, new Response(Response.NOT_FOUND, Response.EMPTY));
                } catch (IOException e) {
                    sendResponse(session, new Response(Response.INTERNAL_ERROR, Response.EMPTY));
                }
            })
            .exceptionally(ex -> {
                log.error("Failed to upsert into the dao", ex);
                return null;
            });
            return;
        }

        final var replicas = topology.replicas(
                ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)), meta.getRf().getFrom());

        handleLocally(replicas, () -> upsertToDao(meta.getId(), meta.getValue()))
                .thenComposeAsync(handled -> getResponsesFromReplicas(replicas, meta, topology, httpClient))
                .whenCompleteAsync((responses, failure) -> ResponseUtils.checkPutResponses(
                        replicas.contains(topology.whoAmI()) ? 1 : 0,
                        meta.getRf().getAck(),
                        responses, session
                ))
                .exceptionally(ex -> {
                    log.error(FutureUtils.FUTURE_ERROR_LOG, ex);
                    return null;
                });
    }

    void delete(@NotNull final HttpSession session,
                @NotNull final MetaRequest meta) {
        if (meta.proxied()) {
            CompletableFuture.runAsync(() -> {
                try {
                    dao.remove(ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)));
                    sendResponse(session, new Response(Response.ACCEPTED, Response.EMPTY));
                } catch (NoSuchElementException e) {
                    sendResponse(session, new Response(Response.NOT_FOUND, Response.EMPTY));
                } catch (IOException e) {
                    sendResponse(session, new Response(Response.INTERNAL_ERROR, Response.EMPTY));
                }
            })
            .exceptionally(ex -> {
                log.error("Failed to remove from the dao", ex);
                return null;
            });
            return;
        }

        final var replicas = topology.replicas(
                ByteBuffer.wrap(meta.getId().getBytes(Charsets.UTF_8)), meta.getRf().getFrom());
        handleLocally(replicas, () -> removeFromDao(meta.getId()))
                .thenComposeAsync(handled -> getResponsesFromReplicas(replicas, meta, topology, httpClient))
                .whenCompleteAsync((responses, failure) -> ResponseUtils.checkDeleteResponses(
                        replicas.contains(topology.whoAmI()) ? 1 : 0,
                        meta.getRf().getAck(),
                        responses, session
                ))
                .exceptionally(ex -> {
                    log.error(FutureUtils.FUTURE_ERROR_LOG, ex);
                    return null;
                });
    }

    /**
     * Runs the action asynchronously if
     * this node contains in {@code replicas}.
     *
     * @param replicas a list of hosts of replicas
     * @param action the action to run
     * @return a {@code CompletableFuture<Boolean>} that contains true if
     *         this node contains in {@code replicas}
     */
    private CompletableFuture<Boolean> handleLocally(@NotNull final List<String> replicas,
                                                     @NotNull final Runnable action) {
        return CompletableFuture.supplyAsync(() -> {
            if (replicas.contains(topology.whoAmI())) {
                action.run();
                return true;
            }
            return false;
        });
    }

    private void removeFromDao(@NotNull final String key) {
        try {
            dao.remove(ByteBuffer.wrap(key.getBytes(Charsets.UTF_8)));
        } catch (IOException e) {
            log.error("[{}] Can't remove {}", topology.whoAmI(), key, e);
        }
    }

    private void upsertToDao(@NotNull final String key,
                             @NotNull final ByteBuffer value) {
        try {
            dao.upsert(ByteBuffer.wrap(key.getBytes(Charsets.UTF_8)), value);
        } catch (IOException e) {
            log.error("[{}] Can't upsert {}={}", topology.whoAmI(), key, value, e);
        }
    }

    private void getFromDao(@NotNull final String key,
                            @NotNull final List<Value> values) {
        try {
            values.add(Value.from(
                    dao.getCell(ByteBuffer.wrap(key.getBytes(Charsets.UTF_8)))));
        } catch (IOException e) {
            log.error("[{}] Can't get {}", topology.whoAmI(), key, e);
        }
    }
}
