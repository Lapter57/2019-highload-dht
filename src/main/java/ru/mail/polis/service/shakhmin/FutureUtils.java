package ru.mail.polis.service.shakhmin;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.service.shakhmin.topology.Topology;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static ru.mail.polis.service.shakhmin.HttpService.PROXY_HEADER_NAME;
import static ru.mail.polis.service.shakhmin.HttpService.PROXY_HEADER_VALUE;

final class FutureUtils {

    private static final Logger log = LoggerFactory.getLogger(FutureUtils.class);
    static final String FUTURE_ERROR_LOG = "Future error";

    private FutureUtils() {
        // Not instantiable
    }

    /**
     * Sends specific requests to the replicas using {@link HttpClient}.
     *
     * @param replicas a list of hosts of replicas
     * @param meta a meta info of a request
     * @return a {@link CompletableFuture} that contains list of responses from the replicas
     *         or empty list if {@code acks == 0}
     */
    @NotNull
    static CompletableFuture<List<HttpResponse<byte[]>>> getResponsesFromReplicas(
            @NotNull final List<String> replicas,
            @NotNull final MetaRequest meta,
            @NotNull final Topology<String> topology,
            @NotNull final HttpClient httpClient) {
        final int acks = replicas.contains(topology.whoAmI())
                ? meta.getRf().getAck() - 1
                : meta.getRf().getAck();
        if (acks > 0) {
            final var futures = new ArrayList<CompletableFuture<HttpResponse<byte[]>>>();
            for (final var node : replicas) {
                if (!topology.isMe(node)) {
                    final var requestBuilder = HttpRequest
                            .newBuilder(URI.create(node + meta.getRequest().getURI()))
                            .setHeader(PROXY_HEADER_NAME, PROXY_HEADER_VALUE);
                    final var body = meta.getRequest().getBody();
                    switch (meta.getMethod()) {
                        case GET:
                            requestBuilder.GET();
                            break;
                        case PUT:
                            requestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
                            break;
                        case DELETE:
                            requestBuilder.DELETE();
                            break;
                        case POST:
                            requestBuilder.POST(HttpRequest.BodyPublishers.ofByteArray(body));
                            break;
                        default:
                            throw new IllegalArgumentException("Service doesn't support method "
                                    + meta.getMethod());
                    }
                    futures.add(httpClient.sendAsync(
                            requestBuilder.build(),
                            HttpResponse.BodyHandlers.ofByteArray()));
                }
            }
            return getFirstResponses(futures, acks);
        }
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

    /**
     * Return {@link CompletableFuture} that is completed normally
     * when enough responses are received from the replicas or
     * the number of non-received responses is no longer enough.
     * The returned CompletableFuture will contains list of
     * received responses.
     *
     * @param futures a list of {@link CompletableFuture}
     * @param acks a count of expected responses
     * @return a {@code CompletableFuture<List<HttpResponse<byte[]>>>}
     */
    private static CompletableFuture<List<HttpResponse<byte[]>>> getFirstResponses(
            @NotNull final List<CompletableFuture<HttpResponse<byte[]>>> futures,
            final int acks) {
        if (futures.size() < acks) {
            throw new IllegalArgumentException("Number of expected responses = "
                    + futures.size() + " but acks = " + acks);
        }
        final int maxFails = futures.size() - acks;
        final var fails = new AtomicInteger(0);
        final var responses = new CopyOnWriteArrayList<HttpResponse<byte[]>>();
        final var result = new CompletableFuture<List<HttpResponse<byte[]>>>();

        final BiConsumer<HttpResponse<byte[]>, Throwable> biConsumer = (value, failure) -> {
            if ((failure != null || value == null) && fails.incrementAndGet() > maxFails) {
                result.complete(responses);
            } else if (!result.isDone() && value != null) {
                responses.add(value);
                if (responses.size() >= acks) {
                    result.complete(responses);
                }
            }
        };
        for (final var future : futures) {
            future.orTimeout(1, TimeUnit.SECONDS)
                    .whenCompleteAsync(biConsumer)
                    .exceptionally(ex -> {
                        log.error(FUTURE_ERROR_LOG, ex);
                        return null;
                    });
        }
        return result;
    }
}
