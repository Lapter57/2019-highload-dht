package ru.mail.polis.service.shakhmin;

import com.google.common.base.Charsets;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.HttpServerConfig;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.http.Param;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public class SimpleHttpServer extends HttpServer implements Service {

    private final DAO dao;

    public SimpleHttpServer(final int port,
                            @NotNull final DAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
    }

    /**
     * This endpoint handles the request to insert, delete
     * and retrieve data from the storage.
     *
     * @param request request
     * @param id id of resource
     * @return
     *
     *  <p><ul>
     *      <li> 200 if GET request and resource is found;
     *      <li> 201 if PUT request and resource is saved to storage;
     *      <li> 202 if DELETE request and resource is removed from storage;
     *      <li> 404 if resource with {@code id} is not found;
     *      <li> 405 if method not allowed;
     *      <li> 500 if internal error;
     *     </ul>
     */
    @Path("/v0/entity")
    public Response entity(final Request request,
                           @Param("id") final String id) {
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        final var key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    return get(key);

                case Request.METHOD_PUT:
                    return upsert(key, ByteBuffer.wrap(request.getBody()));

                case Request.METHOD_DELETE:
                    return delete(key);

                default:
                    return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
            }
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    /**
     * This endpoint return information about the status of server.
     * @param request request
     * @return 200 if GET request and 405 otherwise
     */
    @Path("/v0/status")
    public Response status(final Request request) {
        if (request.getMethod() == Request.METHOD_GET) {
            return Response.ok(Response.EMPTY);
        }
        return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
    }

    private Response get(@NotNull final ByteBuffer key) throws IOException {
        final var value = dao.get(key);
        final var duplicate = value.duplicate();
        final var body = new byte[duplicate.remaining()];
        duplicate.get(body);
        return Response.ok(body);
    }

    private Response upsert(@NotNull final ByteBuffer key,
                            @NotNull final ByteBuffer value) throws IOException {
        dao.upsert(key, value);
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response delete(@NotNull final ByteBuffer key) throws IOException {
        dao.remove(key);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    @Override
    public void handleDefault(final Request request,
                              final HttpSession session) throws IOException {
        final var response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    private static HttpServerConfig getConfig(final int port) {
        if (port <= 1024 || port >= 65535) {
            throw new IllegalArgumentException("Invalid port");
        }
        final var acceptor = new AcceptorConfig();
        acceptor.port = port;
        final var config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{acceptor};
        config.selectors = 4;
        return config;
    }
}
