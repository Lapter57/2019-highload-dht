package ru.mail.polis.service.shakhmin;

import org.jetbrains.annotations.NotNull;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Set;
import java.util.function.Predicate;

@ThreadSafe
public interface ConsistentHash extends Topology<String> {

    void replaceNodeTo(@NotNull final String oldNode,
                       @NotNull final String newNode);

    @NotNull
    Set<String> addNode(@NotNull final String node,
                        @NotNull final Predicate<VNode> predicate);

    @NotNull
    Set<String> removeNode(@NotNull final String node);
}
