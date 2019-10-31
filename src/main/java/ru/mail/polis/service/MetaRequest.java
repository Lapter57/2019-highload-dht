package ru.mail.polis.service;

import one.nio.http.Request;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.service.shakhmin.topology.RF;

import java.nio.ByteBuffer;

public final class MetaRequest {

    @NotNull
    private final Request request;
    @NotNull
    private final RF rf;
    @NotNull
    private final String id;
    @NotNull
    private final ByteBuffer value;
    private final boolean proxied;

    public MetaRequest(@NotNull final Request request,
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

    public MetaRequest(@NotNull final Request request,
                @NotNull final RF rf,
                @NotNull final String id,
                final boolean proxied) {
        this(request, rf, id, ByteBuffer.allocate(0), proxied);
    }

    @NotNull
    public Request getRequest() {
        return request;
    }

    @NotNull
    public RF getRf() {
        return rf;
    }

    @NotNull
    public String getId() {
        return id;
    }

    @NotNull
    public ByteBuffer getValue() {
        return value;
    }

    public boolean proxied() {
        return proxied;
    }
}
