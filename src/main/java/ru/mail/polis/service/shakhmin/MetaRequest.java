package ru.mail.polis.service.shakhmin;

import one.nio.http.Request;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.service.shakhmin.topology.RF;

import java.nio.ByteBuffer;

import static one.nio.http.Request.METHOD_DELETE;
import static one.nio.http.Request.METHOD_GET;
import static one.nio.http.Request.METHOD_POST;
import static one.nio.http.Request.METHOD_PUT;

public final class MetaRequest {

    private static final String PARAM_ID = "id";

    @NotNull private final Request request;
    @NotNull private final RequestMethod method;
    @NotNull private final RF rf;
    @NotNull private final String id;
    @NotNull private final ByteBuffer value;
    private final boolean proxied;

    /**
     * Creates implementation of meta info of request.
     *
     * @param request request
     * @param rf a replication factor
     * @param proxied true if request is proxied
     */
    MetaRequest(@NotNull final Request request,
                @NotNull final RF rf,
                final boolean proxied) {
        this.request = request;
        this.rf = rf;
        this.id = request.getParameter(PARAM_ID).substring(1);
        this.value = request.getBody() == null
                ? ByteBuffer.allocate(0)
                : ByteBuffer.wrap(request.getBody());
        this.proxied = proxied;
        switch (request.getMethod()) {
            case METHOD_GET:
                method = RequestMethod.GET;
                break;
            case METHOD_POST:
                method = RequestMethod.POST;
                break;
            case METHOD_PUT:
                method = RequestMethod.PUT;
                break;
            case METHOD_DELETE:
                method = RequestMethod.DELETE;
                break;
            default:
                throw new IllegalArgumentException("This method is not supported");
        }
    }

    @NotNull
    Request getRequest() {
        return request;
    }

    @NotNull
    RF getRf() {
        return rf;
    }

    @NotNull
    String getId() {
        return id;
    }

    @NotNull
    public ByteBuffer getValue() {
        return value;
    }

    boolean proxied() {
        return proxied;
    }

    @NotNull
    public RequestMethod getMethod() {
        return method;
    }

    public enum RequestMethod {
        GET, POST, HEAD,
        OPTIONS, PUT, DELETE,
        TRACE, CONNECT, PATCH;
    }
}
