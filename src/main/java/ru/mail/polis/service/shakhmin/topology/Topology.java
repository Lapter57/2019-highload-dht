package ru.mail.polis.service.shakhmin.topology;

import org.jetbrains.annotations.NotNull;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

@ThreadSafe
public interface Topology<T> {
    @NotNull
    T primaryFor(@NotNull final ByteBuffer key);

    @NotNull
    List<T> replicas(@NotNull final ByteBuffer key,
                     final int from);

    boolean isMe(@NotNull final T node);

    @NotNull
    T whoAmI();

    @NotNull
    Set<T> all();
}
