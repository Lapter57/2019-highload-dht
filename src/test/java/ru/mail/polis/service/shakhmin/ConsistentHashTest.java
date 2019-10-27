package ru.mail.polis.service.shakhmin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.mail.polis.TestBase;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsistentHashTest extends TestBase {
    private int port0;
    private int port1;
    private Set<String> nodes;
    private ConsistentHash topology0;
    private ConsistentHash topology1;

    private static final int KEYS_COUNT = 10_000_000;

    @BeforeEach
    void beforeEach() {
        port0 = randomPort();
        port1 = randomPort();

        final var node0 = endpoint(port0);
        final var node1 = endpoint(port1);

        nodes = new LinkedHashSet<>(Arrays.asList(node0, node1));
        topology0 = new ConsistentHashImpl(nodes, node0);
        topology1 = new ConsistentHashImpl(nodes, node1);
    }

    @Test
    void consistentData() {
        for (int i = 0; i < KEYS_COUNT; i++) {
            final var key = randomKeyBuffer();
            final var nodeFromTop0 = topology0.primaryFor(key.duplicate());
            final var nodeFromTop1 = topology1.primaryFor(key.duplicate());
            assertEquals(nodeFromTop0, nodeFromTop1);
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
}
