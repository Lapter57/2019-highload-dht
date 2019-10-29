package ru.mail.polis.service.shakhmin;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;

import static ru.mail.polis.service.shakhmin.ReplicatedHttpServer.TIMESTAMP_HEADER;

final class Value {

    private static final Value ABSENT = new Value(null, -1, State.ABSENT);

    @Nullable private final byte[] data;
    private final long timestamp;
    @NotNull private final State state;

    private Value(@Nullable final byte[] data,
                  final long timestamp,
                  @NotNull final State state) {
        this.data = data;
        this.timestamp = timestamp;
        this.state = state;
    }

    static Value present(@NotNull final byte[] data,
                         final long timestamp) {
        return new Value(data, timestamp, State.PRESENT);
    }

    static Value removed(final long timestamp) {
        return new Value(null, timestamp, State.REMOVED);
    }

    static Value absent() {
        return ABSENT;
    }

    public static Value from(@NotNull final Response response) throws IOException {
        final var timestamp = response.getHeader(TIMESTAMP_HEADER);
        if (response.getStatus() == 200) {
            if (timestamp == null) {
                throw new IllegalArgumentException("Wrong input data");
            }
            return present(response.getBody(), Long.parseLong(timestamp));
        } else if (response.getStatus() == 404) {
            if (timestamp == null) {
                return absent();
            } else {
                return removed(Long.parseLong(timestamp));
            }
        } else {
            throw new IOException();
        }
    }

    public static Response transform(@NotNull final Value value,
                                     final boolean proxied) {
        Response result;
        switch (value.getState()) {
            case PRESENT:
                result = new Response(Response.OK, value.getData());
                if (proxied) {
                    result.addHeader(TIMESTAMP_HEADER + value.getTimestamp());
                }
                return result;
            case REMOVED:
                result = new Response(Response.NOT_FOUND, Response.EMPTY);
                if (proxied) {
                    result.addHeader(TIMESTAMP_HEADER + value.getTimestamp());
                }
                return result;
            case ABSENT:
                return new Response(Response.NOT_FOUND, Response.EMPTY);
            default:
                throw new IllegalArgumentException("Wrong input data");
        }
    }

    static Value merge(@NotNull final Collection<Value> values) {
        return values.stream()
                .filter(v -> v.getState() != State.ABSENT)
                .max(Comparator.comparingLong(Value::getTimestamp))
                .orElseGet(Value::absent);
    }

    @Nullable
    byte[] getData() {
        return data;
    }

    long getTimestamp() {
        return timestamp;
    }

    @NotNull
    State getState() {
        return state;
    }

    private enum State {
        PRESENT,
        REMOVED,
        ABSENT
    }
}
