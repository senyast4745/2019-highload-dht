package ru.mail.polis.service.senyast;

import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.senyast.model.Bytes;
import ru.mail.polis.dao.senyast.model.Value;

import java.nio.ByteBuffer;

final class ResponseUtil {
    static final String HEADER_PROXY = "X-Proxied: True";
    private static final String HEADER_TIME_STAMP = "X-Timestamp: ";

    private ResponseUtil() {
    }

    static boolean is2XX(final Response response) {
        return response.getStatus() <= 299 && response.getStatus() >= 200;
    }

    static boolean isProxied(@NotNull final Request request) {
        return request.getHeader(HEADER_PROXY) != null;
    }

    static Response valueToResponse(final Value value) {
        if (value.state() == Value.State.PRESENT) {
            final var response = Response.ok(Bytes.toArray(value.getData()));
            response.addHeader(HEADER_TIME_STAMP + value.getTimestamp());
            return response;
        } else if (value.state() == Value.State.REMOVED) {
            final var response = new Response(Response.NOT_FOUND, Response.EMPTY);
            response.addHeader(HEADER_TIME_STAMP + value.getTimestamp());
            return response;
        }
        return new Response(Response.NOT_FOUND, Response.EMPTY);
    }

    static Value responseToValue(final Response response) {
        final String ts = response.getHeader(HEADER_TIME_STAMP);
        return getValue(response, ts);
    }

    @NotNull
    static Value getValue(Response response, String ts) {
        if (response.getStatus() == 200) {
            if (ts == null) {
                throw new IllegalArgumentException();
            }
            return Value.of(Long.parseLong(ts), ByteBuffer.wrap(response.getBody()));
        } else {
            if (ts == null) {
                return Value.absent();
            }
            return Value.tombstone(Long.parseLong(ts));
        }
    }
}
