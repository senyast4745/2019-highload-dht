package ru.mail.polis.service.senyast;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.senyast.model.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

public class Node implements Topology<String> {

    @NotNull
    private final String[] nodes;
    @NotNull
    private final String name;

    /**
     * Sharding data node.
     *
     * @param set  set of sharding nodes
     * @param name current node name
     */
    public Node(@NotNull final Set<String> set, @NotNull final String name) {
        this.name = name;

        nodes = new String[set.size()];
        set.toArray(nodes);
        Arrays.sort(nodes);
    }

    @Override
    public boolean isMe(@NotNull final String topology) {
        return name.equals(topology);
    }

    @Override
    public String getNodeName(final ByteBuffer key) {
        final int hash = key.hashCode();
        final int index = (hash & Integer.MAX_VALUE) % nodes.length;
        return nodes[index];
    }

    @Override
    public Set<String> all() {
        return Set.of(nodes);
    }

    @Override
    public int size() {
        return nodes.length;
    }

    @Override
    public Set<String> primaryFor(ByteBuffer key, ReplicationFactor replicationFactor) {
        if (replicationFactor.getFrom() > nodes.length) {
            throw new IllegalArgumentException();
        }

        final int hash = key.hashCode();

        final int index = (hash & Integer.MAX_VALUE) % nodes.length;

        return Set.of(Arrays.copyOfRange(nodes, index, index + replicationFactor.getFrom()));
    }

    @Override
    public String me() {
        return name;
    }
}
