package ru.mail.polis.service.shakhmin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.mail.polis.TestBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsistentHashTest extends TestBase {
    private static final int KEYS_COUNT = 1_000_000;

    private int port0;
    private int port1;
    private int port2;
    private Set<String> nodes;
    private ConsistentHash topology0;
    private ConsistentHash topology1;
    private ConsistentHash topology2;

    @BeforeEach
    void beforeEach() {
        port0 = randomPort();
        port1 = randomPort();
        port2 = randomPort();

        final var node0 = endpoint(port0);
        final var node1 = endpoint(port1);
        final var node2 = endpoint(port2);
        
        nodes = new LinkedHashSet<>(Arrays.asList(node0, node1, node2));
        topology0 = new ConsistentHashImpl(nodes, node0);
        topology1 = new ConsistentHashImpl(nodes, node1);
        topology2 = new ConsistentHashImpl(nodes, node2);
    }

    @Test
    void consistentData() {
        for (int i = 0; i < KEYS_COUNT; i++) {
            final var key = randomKeyBuffer();
            final var primary0 = topology0.primaryFor(key.duplicate());
            final var primary1 = topology1.primaryFor(key.duplicate());
            final var primary2 = topology2.primaryFor(key.duplicate());
            assertEquals(primary0, primary1, primary2);
        }
    }

    @Test
    void addNode() {
        assertEquals(nodes, topology0.all());

        final var port = randomPort();
        final var newNode = endpoint(port);
        nodes.add(newNode);
        assertTrue(topology0.addNode(newNode));
        assertEquals(nodes, topology0.all());

        assertFalse(topology0.addNode(newNode));
        assertEquals(nodes, topology0.all());
    }

    @Test
    void removeNode() {
        assertEquals(nodes, topology0.all());

        final var removedNode = endpoint(port0);
        nodes.remove(removedNode);
        assertTrue(topology0.removeNode(removedNode));
        assertEquals(nodes, topology0.all());

        assertFalse(topology0.removeNode(removedNode));
        assertEquals(nodes, topology0.all());
    }

    @Test
    void checkDistribution() {
        final var nodesToNumKeys = new HashMap<String, Integer>();
        for (long i = 0; i < KEYS_COUNT; i++) {
            final var primary = topology0.primaryFor(randomKeyBuffer());
            nodesToNumKeys.compute(primary, (n, c) -> c == null ? 1 : c + 1);
        }
        for (final var entry : nodesToNumKeys.entrySet()) {
            assertTrue(entry.getValue() > KEYS_COUNT / (nodes.size() + 1) );
        }
    }
}
