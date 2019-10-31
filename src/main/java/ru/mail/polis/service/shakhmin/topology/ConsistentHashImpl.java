package ru.mail.polis.service.shakhmin.topology;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.jetbrains.annotations.NotNull;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;


/**
 * See "6.2 Ensuring Uniform Load distribution" (Strategy 3) in
 * <a href="https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf">
 *     Dynamo: Amazon’s Highly Available Key-value Store
 * </a>
 */
public final class ConsistentHashImpl implements ConsistentHash {

    private static final int PARTITIONS_COUNT = 32;
    private static final int HASH_BITS = 64;

    @NotNull
    private final NavigableMap<Long, VNode> ring;

    @NotNull
    private final HashFunction hashFunction;

    @NotNull
    private final NavigableSet<String> nodes;

    @NotNull
    private final String me;

    /**
     * This is an implementation of consistent hashing to distribute
     * the load across multiple storage hosts.
     *
     * <p>See "6.2 Ensuring Uniform Load distribution" (Strategy 3) in
     * <a href="https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf">
     *     Dynamo: Amazon’s Highly Available Key-value Store
     * </a>
     *
     * @param nodes set of addresses of nodes
     * @param me address of current node
     */
    public ConsistentHashImpl(@NotNull final Set<String> nodes,
                              @NotNull final String me) {
        this.ring = new TreeMap<>();
        this.hashFunction = Hashing.murmur3_128();
        this.me = me;
        this.nodes = new TreeSet<>(nodes);
        generateVNodes(new ArrayList<>(this.nodes));
    }

    private void generateVNodes(@NotNull final List<String> nodes) {
        for (int i = 0; i < PARTITIONS_COUNT; i++) {
            final long token =
                    (long) ((Math.pow(2, HASH_BITS) / PARTITIONS_COUNT) * i - Math.pow(2, HASH_BITS - 1));
            ring.put(token, new VNode(token, nodes.get(i % nodes.size())));
        }
    }

    @NotNull
    @Override
    public String primaryFor(@NotNull final ByteBuffer key) {
        return firstReplica(key).getValue().getAddress();
    }

    @NotNull
    @Override
    public List<String> replicas(@NotNull final ByteBuffer key,
                                 final int from) {
        if (from > nodes.size()) {
            throw new IllegalArgumentException(
                    "Wrong RF: [from = " + from + "] > [ nodesNumber = " + all().size());
        }
        final var replicas = new ArrayList<String>();
        final var firstReplica = firstReplica(key);
        replicas.add(firstReplica.getValue().getAddress());
        int cntReplicas = 1;
        var iter = ring.tailMap(firstReplica.getKey()).values().iterator();
        while (cntReplicas != from) {
            if (!iter.hasNext()) {
                iter = ring.values().iterator();
            }
            var vnode = iter.next();
            final var replica = vnode.getAddress();
            if (!replicas.contains(replica)) {
                replicas.add(replica);
                cntReplicas++;
            }
        }
        return replicas;
    }

    @NotNull
    private Map.Entry<Long, VNode> firstReplica(@NotNull final ByteBuffer key) {
        final long hashKey = hashFunction.hashBytes(key).asLong();
        final var entry = ring.ceilingEntry(hashKey);
        return entry == null
                ? ring.firstEntry()
                : entry;
    }

    @Override
    public boolean isMe(@NotNull final String node) {
        return me.equals(node);
    }

    @NotNull
    @Override
    public String whoAmI() {
        return me;
    }

    @NotNull
    @Override
    public Set<String> all() {
        return Set.copyOf(nodes);
    }

    @Override
    public boolean addNode(@NotNull final String node) {
        if (nodes.contains(node)) {
            return false;
        }
        nodes.add(node);
        final var vNodes = pickRandomElements(ring.values(), PARTITIONS_COUNT / nodes.size());
        for (final var vn : vNodes) {
            vn.setAddress(node);
        }
        return true;
    }

    @Override
    public boolean removeNode(@NotNull final String node) {
        if (!nodes.contains(node)) {
            return false;
        }
        nodes.remove(node);
        ring.replaceAll((t, vn) -> {
            if (node.equals(vn.getAddress())) {
                vn.setAddress(pickRandomElements(all(), 1).get(0));
            }
            return vn;
        });
        return true;
    }

    private static <T> List<T> pickRandomElements(@NotNull final Collection<T> coll,
                                                  final int count) {
        final var elements = new ArrayList<>(coll);
        Collections.shuffle(elements);
        return elements.subList(0, count);
    }
}
