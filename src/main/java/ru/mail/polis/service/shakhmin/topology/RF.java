package ru.mail.polis.service.shakhmin.topology;

import com.google.common.base.Splitter;
import org.jetbrains.annotations.NotNull;

public class RF {
    private final int ack;
    private final int from;

    private RF(final int ack,
               final int from) {
        this.ack = ack;
        this.from = from;
    }

    private static int quorum(final int numNodes) {
        return numNodes / 2 + 1;
    }

    public static RF from(@NotNull final String replicas) {
        final var splitted = Splitter.on('/').splitToList(replicas);
        if (splitted.size() != 2) {
            throw new IllegalArgumentException("Wrong RF: " + replicas);
        }
        final int ack = Integer.parseInt(splitted.get(0));
        final int from = Integer.parseInt(splitted.get(1));
        if (ack < 1 || from < ack) {
            throw new IllegalArgumentException("Wrong RF: " + replicas);
        }
        return new RF(ack,from);
    }

    public static RF from(final int numNodes) {
        return new RF(RF.quorum(numNodes), numNodes);
    }

    public int getAck() {
        return ack;
    }

    public int getFrom() {
        return from;
    }
}
