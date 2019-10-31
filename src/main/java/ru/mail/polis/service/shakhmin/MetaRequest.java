package ru.mail.polis.service.shakhmin;

import one.nio.http.Request;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.service.shakhmin.topology.RF;
import java.nio.ByteBuffer;

public final class MetaRequest {

    private static final String PARAM_ID = "id";

    @NotNull
    private final Request request;
    @NotNull
    private final RF rf;
    @NotNull
    private final String id;
    @NotNull
    private final ByteBuffer value;
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
}
