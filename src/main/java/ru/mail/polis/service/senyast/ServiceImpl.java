package ru.mail.polis.service.senyast;

import com.google.common.base.Charsets;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ServiceImpl extends HttpServer implements Service {

    private final DAO dao;
    private final Topology<String> topology;
    private final Map<String, HttpClient> pool;
    @NotNull
    private final Executor executors;
    private static Logger log = LoggerFactory.getLogger(ServiceImpl.class);


    /**
     * Constructor to my Impl of Service.
     *
     * @param port     httpServer port
     * @param dao      implementation of DAO
     * @param executor thread executor
     * @throws IOException if server can not start
     */
    public ServiceImpl(final int port, @NotNull final DAO dao, @NotNull final Executor executor,
                       final Topology<String> topology) throws IOException {
        super(getServerConfig(port));
        this.dao = dao;
        this.executors = executor;
        this.topology = topology;

        pool = new HashMap<>(topology.size() >> 1);
        for (final String name : topology.all()) {
            if (!this.topology.isMe(name)) {
                pool.put(name, new HttpClient(new ConnectionString(name + "?timeout=100")));
            }
        }
    }

    private static HttpServerConfig getServerConfig(final int port) {
        final AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;
        acceptor.deferAccept = true;

        final HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{acceptor};
        config.minWorkers = 4;
        config.maxWorkers = 8;
        return config;
    }

    /**
     * Method to map to "/v0/entity" URL.
     *
     * @param request request to Server
     * @param id      key
     * @param session http Session of request
     */
    @SuppressWarnings("unused")
    @Path("/v0/entity")
    public void daoMethods(@NotNull final Request request,
                           @Param("id") final String id,
                           final HttpSession session) {
        if (id == null || id.isEmpty()) {
            sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));

        final String workerNode = topology.getNodeName(key);

        if (!topology.isMe(workerNode)) {
            executeAsync(session, () -> proxy(workerNode, request));
            return;
        }

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                executeAsync(session, () -> getMethod(key));
                break;
            case Request.METHOD_PUT:
                executeAsync(session, () -> putMethod(key, request));
                break;
            case Request.METHOD_DELETE:
                executeAsync(session, () -> deleteMethod(key));
                break;
            default:
                sendResponse(session, new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY));
                break;
        }
    }

    /**
     * Method to check Server status.
     *
     * @param session http Session of request
     */
    @SuppressWarnings("unused")
    @Path("/v0/status")
    public void status(final HttpSession session) {
        sendResponse(session, new Response(Response.OK, Response.EMPTY));
    }


    /**
     * Method to get more key - value pair.
     *
     * @param request request to Server
     * @param session http Session of request
     * @param start   request parameter "start" - start of Iterator - required
     * @param end     request parameter "end" - end of Iterator - non required
     */
    @SuppressWarnings("unused")
    @Path("/v0/entities")
    public void entities(final Request request, final HttpSession session, @Param("start") final String start,
                         @Param("end") final String end) {
        if (start == null || start.isEmpty()) {
            sendResponse(session, new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }

        try {
            final Iterator<Record> records = dao.range(ByteBuffer.wrap(start.getBytes(UTF_8)),
                    end == null || end.isEmpty() ? null : ByteBuffer.wrap(end.getBytes(UTF_8)));
            ((StorageSession) session).stream(records);
        } catch (IOException e) {
            log.error("Entities sending exception", e);
        }
    }

    @Override
    public HttpSession createSession(final Socket socket) {
        return new StorageSession(socket, this);
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        final Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    private Response getMethod(final ByteBuffer key) throws IOException {
        final ByteBuffer value = dao.get(key);
        final ByteBuffer duplicate = value.duplicate();
        final byte[] body = new byte[duplicate.remaining()];
        duplicate.get(body);
        return new Response(Response.OK, body);
    }

    private Response deleteMethod(final ByteBuffer key) throws IOException {
        dao.remove(key);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response putMethod(final ByteBuffer key, final Request request) throws IOException {
        dao.upsert(key, ByteBuffer.wrap(request.getBody()));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response proxy(final String workerNode, final Request request) {
        try {
            return pool.get(workerNode).invoke(request);
        } catch (InterruptedException | PoolException | HttpException | IOException e) {
            log.error("Request proxy error ", e);
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
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
            log.error("IOException in sending response", e);
            try {
                session.sendError(Response.INTERNAL_ERROR, "Error while send response");
            } catch (IOException ex) {
                log.error("Error while send response error");
            }
        }
    }

    @FunctionalInterface
    interface Action {
        Response act() throws IOException;
    }
}
