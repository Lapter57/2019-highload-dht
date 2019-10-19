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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

public class AsyncHttpServer extends HttpServer implements Service {

    private static final Logger log = LoggerFactory.getLogger(AsyncHttpServer.class);

    @NotNull
    private final DAO dao;

    @NotNull
    private final Executor serverWorkers;

    /**
     * Creates an asynchronous http server for working with storage.
     *
     * @param port port of server
     * @param dao storage
     * @param workers thread pool for request processing
     * @throws IOException if an I/O error occurs
     */
    public AsyncHttpServer(final int port,
                           @NotNull final DAO dao,
                           @NotNull final Executor workers) throws IOException {
        super(getConfig(port));
        this.dao = dao;
        this.serverWorkers = workers;
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
                       @Param("id") final String id) {
        if (id == null || id.isEmpty()) {
            sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                executeAsync(session, () -> get(id));
                break;

            case Request.METHOD_PUT:
                executeAsync(session, () -> upsert(id, ByteBuffer.wrap(request.getBody())));
                break;

            case Request.METHOD_DELETE:
                executeAsync(session, () -> delete(id));
                break;

            default:
                sendResponse(session, new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY));
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
                         @Param("end") String end) {
        if (start == null || start.isEmpty()) {
            sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }

        if (request.getMethod() != Request.METHOD_GET) {
            sendResponse(session, new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY));
            return;
        }

        try {
            final Iterator<Record> records =
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

    private Response get(@NotNull final String id) {
        try {
            final ByteBuffer value = dao.get(ByteBuffer.wrap(id.getBytes(Charsets.UTF_8)));
            final var duplicate = value.duplicate();
            final var body = new byte[duplicate.remaining()];
            duplicate.get(body);
            return Response.ok(body);
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    private Response upsert(@NotNull final String id,
                            @NotNull final ByteBuffer value) {
        try {
            dao.upsert(ByteBuffer.wrap(id.getBytes(Charsets.UTF_8)), value);
            return new Response(Response.CREATED, Response.EMPTY);
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }  catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    private Response delete(@NotNull final String id) {
        try {
            dao.remove(ByteBuffer.wrap(id.getBytes(Charsets.UTF_8)));
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }  catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }

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
                              @NotNull final Supplier<Response> action) {
        serverWorkers.execute(() -> sendResponse(session, action.get()));
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
