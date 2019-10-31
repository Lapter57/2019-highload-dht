package ru.mail.polis.service.shakhmin;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.shakhmin.LSMDao;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.shakhmin.topology.RF;
import ru.mail.polis.service.shakhmin.topology.Topology;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;

public class ReplicatedHttpServer extends HttpServer implements Service {

    private static final Logger log = LoggerFactory.getLogger(ReplicatedHttpServer.class);
    private static final String PROXY_HEADER = "X-OK-Proxy: true";
    static final String TIMESTAMP_HEADER = "X-OK-Timestamp: ";

    @NotNull
    private final Topology<String> topology;

    @NotNull
    private final LSMDao dao;

    @NotNull
    private final Executor serverWorkers;

    @NotNull
    private final CompletionService<Response> completionService;

    @NotNull
    private final Map<String, HttpClient> pool;

    @NotNull
    private RF defaultRF;

    /**
     * Creates an replicated http server for working with storage.
     *
     * @param port port of server
     * @param dao storage
     * @param workers thread pool for request processing
     * @throws IOException if an I/O error occurs
     */
    public ReplicatedHttpServer(final int port,
                                @NotNull final Topology<String> topology,
                                @NotNull final DAO dao,
                                @NotNull final Executor workers) throws IOException {
        super(getConfig(port));
        this.topology = topology;
        this.dao = (LSMDao) dao;
        this.serverWorkers = workers;
        completionService = new ExecutorCompletionService<>(workers);

        final var nodes = topology.all();
        this.defaultRF = RF.from(nodes.size());
        this.pool = new HashMap<>();
        for (final var node : nodes) {
            if (topology.isMe(node)) {
                continue;
            }
            pool.put(node, new HttpClient(new ConnectionString(node + "?timeout=100")));
        }
    }

    /**
     * This endpoint handles the request to insert, delete
     * and retrieve data from the storage.
     *
     * @param session http session
     * @param request request
     * @param id id of resource
     */
    @Path("/v0/entity")
    public void entity(final HttpSession session,
                       final Request request,
                       @Param("id") final String id,
                       @Param("replicas") final String replicas) {
        if (id == null || id.isEmpty()) {
            sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }

        final RF rf;
        try {
            rf = replicas == null ? defaultRF : RF.from(replicas);
            final int nodesNumber = topology.all().size();
            if (rf.getFrom() > nodesNumber) {
                throw new IllegalArgumentException(
                        "Wrong RF: [from = " + rf.getFrom() + "] > [ nodesNumber = " + nodesNumber);
            }
        } catch (IllegalArgumentException e) {
            sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }

        final boolean proxied = request.getHeader(PROXY_HEADER) != null;

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                executeAsync(session, () -> get(new MetaRequest(request, rf, id, proxied)));
                break;

            case Request.METHOD_PUT:
                executeAsync(session,
                        () -> upsert(new MetaRequest(
                                request, rf, id,  ByteBuffer.wrap(request.getBody()), proxied)));
                break;

            case Request.METHOD_DELETE:
                executeAsync(session,
                        () -> delete(new MetaRequest(request, rf, id, proxied)));
                break;

            default:
                sendResponse(session, new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY));
                break;
        }
    }

    /**
     * This endpoint handles the request
     * retrieve range of data from the storage.
     *
     * @param session http session
     * @param request request
     * @param start start point of range data
     * @param end end point of range data
     */
    @Path("/v0/entities")
    public void entities(final HttpSession session,
                         final Request request,
                         @Param("start") final String start,
                         @Param("end") final String end) {
        if (start == null || start.isEmpty()) {
            sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }

        if (request.getMethod() != Request.METHOD_GET) {
            sendResponse(session, new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY));
            return;
        }

        try {
            final var records =
                    dao.range(
                            ByteBuffer.wrap(start.getBytes(Charsets.UTF_8)),
                            end == null || end.isEmpty() ? null : ByteBuffer.wrap(end.getBytes(Charsets.UTF_8)));
            ((StorageSession) session).stream(records);
        } catch (IOException e) {
            sendResponse(session, new Response(Response.INTERNAL_ERROR, Response.EMPTY));
        }
    }

    private Response proxy(@NotNull final String node,
                           @NotNull final Request request) throws IOException {
        try {
            request.addHeader(PROXY_HEADER);
            return pool.get(node).invoke(request);
        } catch (InterruptedException | PoolException | IOException | HttpException e) {
            throw new IOException("Can't proxy", e);
        }
    }

    /**
     * This endpoint return information about the status of server.
     *
     * @return 200 OK
     */
    @Path("/v0/status")
    public Response status() {
        return new Response(Response.OK, Response.EMPTY);
    }

    private void sendResponse(@NotNull final HttpSession session,
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

    private Response get(@NotNull final MetaRequest meta) {
        if (meta.proxied) {
            try {
                return Value.transform(
                        Value.from(dao.getCell(ByteBuffer.wrap(meta.id.getBytes(Charsets.UTF_8)))), true);
            } catch (IOException e) {
                return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
            }
        }

        final var replicas = topology.replicas(
                ByteBuffer.wrap(meta.id.getBytes(Charsets.UTF_8)), meta.rf.getFrom());

        int acks = 0;
        final var values = new ArrayList<Value>();
        if (replicas.contains(topology.whoAmI())) {
            try {
                values.add(Value.from(dao.getCell(ByteBuffer.wrap(meta.id.getBytes(Charsets.UTF_8)))));
                acks++;
            } catch (IOException e) {
                log.error("[{}] Can't get {}", topology.whoAmI(), meta.id, e);
            }
        }

        for (final var response : getResponsesFromRelicas(replicas, meta)) {
            try {
                values.add(Value.from(response));
                acks++;
            } catch (IllegalArgumentException e) {
                log.error("[{}] Bad response", topology.whoAmI(), e);
            }
        }

        if (acks >= meta.rf.getAck()) {
            return Value.transform(Value.merge(values), false);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response upsert(@NotNull final MetaRequest meta) {
        if (meta.proxied) {
            try {
                dao.upsert(ByteBuffer.wrap(meta.id.getBytes(Charsets.UTF_8)), meta.value);
                return new Response(Response.CREATED, Response.EMPTY);
            } catch (NoSuchElementException e) {
                return new Response(Response.NOT_FOUND, Response.EMPTY);
            } catch (IOException e) {
                return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
            }
        }

        final var replicas = topology.replicas(
                ByteBuffer.wrap(meta.id.getBytes(Charsets.UTF_8)), meta.rf.getFrom());

        int acks = 0;
        if (replicas.contains(topology.whoAmI())) {
            try {
                dao.upsert(ByteBuffer.wrap(meta.id.getBytes(Charsets.UTF_8)), meta.value);
                acks++;
            } catch (IOException e) {
                log.error("[{}] Can't upsert {}={}",
                        topology.whoAmI(), meta.id, meta.value, e);
            }
        }

        for (final var response : getResponsesFromRelicas(replicas, meta)) {
            if (response.getStatus() == 201) {
                acks++;
            }
        }

        if (acks >= meta.rf.getAck()) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response delete(@NotNull final MetaRequest meta) {
        if (meta.proxied) {
            try {
                dao.remove(ByteBuffer.wrap(meta.id.getBytes(Charsets.UTF_8)));
                return new Response(Response.ACCEPTED, Response.EMPTY);
            } catch (NoSuchElementException e) {
                return new Response(Response.NOT_FOUND, Response.EMPTY);
            } catch (IOException e) {
                return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
            }
        }

        final var replicas = topology.replicas(
                ByteBuffer.wrap(meta.id.getBytes(Charsets.UTF_8)), meta.rf.getFrom());

        int acks = 0;
        if (replicas.contains(topology.whoAmI())) {
            try {
                dao.remove(ByteBuffer.wrap(meta.id.getBytes(Charsets.UTF_8)));
                acks++;
            } catch (IOException e) {
                log.error("[{}] Can't remove {}={}",
                        topology.whoAmI(), meta.id, meta.value, e);
            }
        }

        for (final var response : getResponsesFromRelicas(replicas, meta)) {
            if (response.getStatus() == 202) {
                acks++;
            }
        }

        if (acks >= meta.rf.getAck()) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private List<Response> getResponsesFromRelicas(@NotNull final List<String> replicas,
                                                   @NotNull final MetaRequest meta) {
        int acks = 0;
        if (replicas.contains(topology.whoAmI())) {
            acks++;
        }
        for (final var node: replicas) {
            if (!topology.isMe(node)) {
                completionService.submit(() -> proxy(node, meta.request));
            }
        }
        int failures = 0;
        final var responses = new ArrayList<Response>();
        while (acks + failures != replicas.size()) {
            try {
                final var response = completionService.take().get();
                responses.add(response);
                if (response.getStatus() >= 200 && response.getStatus() < 300) {
                    acks++;
                } else {
                    failures++;
                }
            } catch (InterruptedException e) {
                log.error("[{}] Worker was interrupted while proxy", topology.whoAmI(), e);
                failures++;
            } catch (ExecutionException e) {
                log.error("[{}] Failed computation while proxy", topology.whoAmI(), e);
                failures++;
            }
        }
        return responses;
    }

    @Override
    public void handleDefault(final Request request,
                              final HttpSession session) throws IOException {
        final var response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    @Override
    public HttpSession createSession(final Socket socket) {
        return new StorageSession(socket, this);
    }

    private void executeAsync(@NotNull final HttpSession session,
                              @NotNull final Action action) {
        serverWorkers.execute(() -> {
            try {
                sendResponse(session, action.act());
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, "Internal error");
                } catch (IOException ex) {
                    log.error("Error with send response {}", ex.getMessage());
                }
            }
        });
    }

    private static HttpServerConfig getConfig(final int port) {
        if (port <= 1024 || port >= 65535) {
            throw new IllegalArgumentException("Invalid port");
        }

        final var acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;
        acceptor.deferAccept = true;

        final var config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{acceptor};
        return config;
    }

    private final static class MetaRequest {

        @NotNull final Request request;
        @NotNull final RF rf;
        @NotNull final String id;
        @NotNull final ByteBuffer value;
        final boolean proxied;

        MetaRequest(@NotNull final Request request,
                    @NotNull final RF rf,
                    @NotNull final String id,
                    @NotNull final ByteBuffer value,
                    final boolean proxied) {
            this.request = request;
            this.rf = rf;
            this.id = id;
            this.value = value;
            this.proxied = proxied;
        }

        MetaRequest(@NotNull final Request request,
                    @NotNull final RF rf,
                    @NotNull final String id,
                    final boolean proxied) {
            this(request, rf, id, ByteBuffer.allocate(0), proxied);
        }
    }

    @FunctionalInterface
    interface Action {
        Response act() throws IOException;
    }
}