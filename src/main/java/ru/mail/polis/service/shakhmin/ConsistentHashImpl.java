package ru.mail.polis.service.shakhmin;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

public final class ConsistentHashImpl implements ConsistentHash {

    private static final int PARTITIONS_COUNT = 32;
    private static final int HASH_BITS = 64;

    @NotNull
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    @NotNull
    private final SortedMap<Long, VNode> ring;

    @NotNull
    private final HashFunction hashFunction;

    @NotNull
    private final List<String> nodes;

    @NotNull
    private final String me;

    public ConsistentHashImpl(@NotNull final Set<String> nodes,
                              @NotNull final String me) {
        this.ring = new TreeMap<>();
        this.hashFunction = Hashing.murmur3_128();
        this.me = me;
        this.nodes = new ArrayList<>(nodes);
        this.nodes.sort(String::compareTo);
        generateVNodes(this.nodes);
    }

    private void generateVNodes(@NotNull final List<String> nodes) {
        lock.writeLock().lock();
        try {
            for (int i = 0; i < PARTITIONS_COUNT; i++) {
                final long token =
                        (long) ((Math.pow(2, HASH_BITS) / PARTITIONS_COUNT) * i - Math.pow(2, HASH_BITS - 1));
                ring.put(token, new VNode(token, nodes.get(i % nodes.size())));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void replaceNodeTo(@NotNull final String oldNode,
                              @NotNull final String newNode) {
        lock.writeLock().lock();
        try {
            ring.replaceAll((t, vn) -> {
                if (oldNode.equals(vn.getAddress())) {
                    vn.setAddress(newNode);
                }
                return vn;
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    @NotNull
    @Override
    public Set<String> addNode(@NotNull final String node,
                               @NotNull final Predicate<VNode> predicate) {
        lock.writeLock().lock();
        try {
            final var replacedNodes = new HashSet<String>();
            ring.replaceAll((t, vn) -> {
                if (predicate.test(vn)) {
                    replacedNodes.add(vn.getAddress());
                    vn.setAddress(node);
                }
                return vn;
            });
            return replacedNodes;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @NotNull
    @Override
    public Set<String> removeNode(@NotNull final String node) {
        lock.writeLock().lock();
        try {
            final var extendedNodes = new HashSet<String>();
            var modifiedVNodes = new ArrayList<VNode>();
            for (final var vNode : ring.values()) {
                if (!node.equals(vNode.getAddress()) && !modifiedVNodes.isEmpty()) {
                    for (final var vn : modifiedVNodes) {
                        vn.setAddress(vNode.getAddress());
                        modifiedVNodes = new ArrayList<>() ;
                    }
                    extendedNodes.add(vNode.getAddress());
                } else if (node.equals(vNode.getAddress())) {
                    modifiedVNodes.add(vNode);
                }
            }
            return extendedNodes;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @NotNull
    @Override
    public String primaryFor(@NotNull final ByteBuffer key) {
        lock.readLock().lock();
        try {
            final long hashKey = hashFunction.hashBytes(key).asLong();
            final var tailMap = ring.tailMap(hashKey);
            final var token = !tailMap.isEmpty() ? tailMap.firstKey() : ring.firstKey();
            return ring.get(token).getAddress();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean isMe(@NotNull final String node) {
        lock.readLock().lock();
        try {
            return me.equals(node);
        } finally {
            lock.readLock().unlock();
        }
    }

    @NotNull
    @Override
    public Set<String> all() {
        lock.readLock().lock();
        try {
            return Set.copyOf(nodes);
        } finally {
            lock.readLock().unlock();
        }
    }
}
