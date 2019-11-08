package ru.mail.polis.service.senyast;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.util.Set;

@ThreadSafe
public interface Topology<T> {

    boolean isMe(String topology);

    String getNodeName(ByteBuffer key);

    Set<T> all();

    int size();

    Set<T> primaryFor(ByteBuffer key, ReplicationFactor replicationFactor);

    T me();
}
