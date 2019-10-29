package ru.mail.polis.service.shakhmin.topology;

import org.jetbrains.annotations.NotNull;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface ConsistentHash extends Topology<String> {

    boolean addNode(@NotNull final String node);

    boolean removeNode(@NotNull final String node);
}
