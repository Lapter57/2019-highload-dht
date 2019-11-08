package ru.mail.polis.service.shakhmin;

import one.nio.http.HttpSession;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

final class ResponseUtils {

    private static final Logger log = LoggerFactory.getLogger(ResponseUtils.class);

    private ResponseUtils() {
        // Not instantiable
    }

    public static void sendResponse(@NotNull final HttpSession session,
                             @NotNull final Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            try {
                session.sendError(Response.INTERNAL_ERROR, "Internal error");
            } catch (IOException ex) {
                log.error("Error with send response {}", ex.getMessage());
            }
        }
    }

    public static void checkPutResponses(final int startAcks,
                                      final int expectedAcks,
                                      @NotNull final List<HttpResponse<byte[]>> responses,
                                      @NotNull final HttpSession session) {
        final int acks = countAcks(
                startAcks,
                responses, r -> r.statusCode() == 201);
        checkAcks(acks, expectedAcks,
                () -> new Response(Response.CREATED, Response.EMPTY),
                session);
    }

    public static void checkDeleteResponses(final int startAcks,
                                         final int expectedAcks,
                                         @NotNull final List<HttpResponse<byte[]>> responses,
                                         @NotNull final HttpSession session) {
        final int acks = countAcks(
                startAcks,
                responses, r -> r.statusCode() == 202);
        checkAcks(acks, expectedAcks,
                () -> new Response(Response.ACCEPTED, Response.EMPTY),
                session);
    }

    public static void checkGetResponses(final int startAcks,
                                      final int expectedAcks,
                                      @NotNull final List<Value> values,
                                      @NotNull final List<HttpResponse<byte[]>> responses,
                                      @NotNull final HttpSession session) {
        final int acks = countAcks(
                startAcks,
                responses, r -> values.add(Value.from(r)));
        checkAcks(acks, expectedAcks,
                () -> Value.transform(Value.merge(values), false),
                session);
    }

    /**
     * Counts the number of successful responses.
     *
     * @param startAcks an initial value of acks
     * @param responses a list of {@code HttpResponse<byte[]>}
     * @param successful a {@code Predicate<HttpResponse<byte[]>} that checks
     *                   if response is successful
     * @return the number of successful responses
     */
    private static int countAcks(final int startAcks,
                                 @NotNull final List<HttpResponse<byte[]>> responses,
                                 @NotNull final Predicate<HttpResponse<byte[]>> successful) {
        int acks = startAcks;
        for (final var response : responses) {
            if (successful.test(response)) {
                acks++;
            }
        }
        return acks;
    }

    /**
     * Sends a specific response if {@code acks == expectedAcks}
     * else sends the error 504 Gateway Timeout.
     *
     * @param acks acks
     * @param expectedAcks an expected value of acks
     * @param supplier a {@code Supplier<Response>} whose a returned {@link Response} is used
     *                 when {@code acks >= expectedAcks}
     * @param session a {@link HttpSession}
     */
    private static void checkAcks(final int acks,
                                  final int expectedAcks,
                                  @NotNull final Supplier<Response> supplier,
                                  @NotNull final HttpSession session) {
        if (acks >= expectedAcks) {
            sendResponse(session, supplier.get());
        } else {
            sendResponse(session, new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
        }
    }
}
