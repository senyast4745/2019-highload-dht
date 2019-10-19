package ru.mail.polis.service.senyast;

import com.google.common.base.Charsets;
import one.nio.http.*;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ServiceImpl extends HttpServer implements Service {

    private final DAO dao;
    @NotNull
    private final Executor executors;
    private static Logger log = LoggerFactory.getLogger(ServiceImpl.class);

    public ServiceImpl(int port, @NotNull DAO dao, @NotNull Executor executor) throws IOException {
        super(getServerConfig(port));
        this.dao = dao;
        this.executors = executor;
    }

    private static HttpServerConfig getServerConfig(int port) {
        AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;
        acceptor.deferAccept = true;

        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{acceptor};
        config.minWorkers = 4;
        config.maxWorkers = 8;
        return config;
    }

    @SuppressWarnings("unused")
    @Path("/v0/entity")
    public void daoMethods(@NotNull final Request request,
                           @Param("id") final String id,
                           HttpSession session) {
        if (id == null || id.isEmpty()) {
            sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    executeAsync(session, () -> getMethod(key));
                    break;
                case Request.METHOD_PUT:
                    executeAsync(session, () -> putMethod(key, request));
                    break;
                case Request.METHOD_DELETE:
                    dao.remove(key);
                    executeAsync(session, () -> new Response(Response.ACCEPTED, Response.EMPTY));
                    break;
                default:
                    executeAsync(session, () -> new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY));
                    break;
            }
        } catch (IOException e) {
            sendResponse(session, new Response(Response.INTERNAL_ERROR, Response.EMPTY));
        }
    }

    @SuppressWarnings("unused")
    @Path("/v0/status")
    public void status(HttpSession session) {
        sendResponse(session, new Response(Response.OK, Response.EMPTY));
    }

    @Path("/v0/entities")
    public void entities(Request request, HttpSession session, @Param("start") String start,
                         @Param("end") String end) {
        if (start == null || start.isEmpty()) {
            sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        if (end != null && end.isEmpty()) {
            end = null;
        }

        try {
            final Iterator<Record> records = dao.range(ByteBuffer.wrap(start.getBytes(UTF_8)),
                    end == null ? null : ByteBuffer.wrap(end.getBytes(UTF_8)));
            ((StorageSession) session).stream(records);
        } catch (IOException e) {
            log.error("Entities sending exception", e);
        }
    }

    @Override
    public HttpSession createSession(Socket socket) {
        return new StorageSession(socket, this);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    private Response getMethod(ByteBuffer key) throws IOException {
        ByteBuffer value = dao.get(key);
        ByteBuffer duplicate = value.duplicate();
        byte[] body = new byte[duplicate.remaining()];
        duplicate.get(body);
        return new Response(Response.OK, body);
    }

    private Response putMethod(ByteBuffer key, Request request) throws IOException {
        dao.upsert(key, ByteBuffer.wrap(request.getBody()));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private void executeAsync(@NotNull final HttpSession session, @NotNull final Action action) {
        executors.execute(() -> {
            try {
                sendResponse(session, action.act());
            } catch (NoSuchElementException e) {
                sendResponse(session, new Response(Response.NOT_FOUND, Response.EMPTY));
            } catch (IOException e) {
                log.error("Execute exception", e);
            }
        });
    }

    private static void sendResponse(@NotNull final HttpSession session,
                                     @NotNull final Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            try {
                session.sendError(Response.INTERNAL_ERROR, "Error while send response");
            } catch (IOException ex) {
                log.error("Error while send error");
            }
        }
    }

    @FunctionalInterface
    interface Action {
        Response act() throws IOException;
    }
}
