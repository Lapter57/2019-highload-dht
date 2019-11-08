package ru.mail.polis.service.shakhmin;

import com.google.common.base.Charsets;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.shakhmin.LSMDao;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.shakhmin.topology.RF;
import ru.mail.polis.service.shakhmin.topology.Topology;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

import static ru.mail.polis.service.shakhmin.HttpService.PROXY_HEADER;
import static ru.mail.polis.service.shakhmin.ResponseUtils.sendResponse;

public class ReplicatedHttpServer extends HttpServer implements Service {

    @NotNull private final Topology<String> topology;
    @NotNull private final LSMDao dao;
    @NotNull private final Executor serverWorkers;
    @NotNull private final RF defaultRF;
    @NotNull private final HttpService httpService;

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
                                @NotNull final Executor workers,
                                @NotNull final Executor proxyWorkers) throws IOException {
        super(getConfig(port));
        this.topology = topology;
        this.dao = (LSMDao) dao;
        this.serverWorkers = workers;

        final var nodes = topology.all();
        this.defaultRF = RF.from(nodes.size());
        this.httpService = new HttpService(proxyWorkers, dao, topology);
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
                executeAsync(() -> httpService.get(session, new MetaRequest(request, rf, proxied)));
                break;
            case Request.METHOD_PUT:
                executeAsync(() -> httpService.upsert(session, new MetaRequest(request, rf, proxied)));
                break;
            case Request.METHOD_DELETE:
                executeAsync(() -> httpService.delete(session, new MetaRequest(request, rf, proxied)));
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

    /**
     * This endpoint return information about the status of server.
     *
     * @return 200 OK
     */
    @Path("/v0/status")
    public Response status() {
        return new Response(Response.OK, Response.EMPTY);
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

    private void executeAsync(@NotNull final Runnable action) {
        serverWorkers.execute(action);
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
}
