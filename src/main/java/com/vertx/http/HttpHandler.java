package com.vertx.http;

import com.lsmdb.service.LSMService;
import io.vertx.core.Vertx;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Optional;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

@Slf4j
public class HttpHandler {

        private final LSMService lsmService;
        private final Vertx vertx;

        public HttpHandler(LSMService lsmService, Vertx vertx) {
                this.lsmService = lsmService;
                this.vertx = vertx;
        }

        public void handleUpdate(final RoutingContext context) {
                final var id = context.pathParam("id");
                final var timestamp = context.pathParam("timestamp");
                // TODO : Verify payload for present of timestamp and id
                final var payload = context.body().buffer();
                vertx.executeBlocking(
                        fut -> fut.complete(lsmService.insert(id, Instant.parse(timestamp), payload)),
                        false,
                        res -> {
                                if (res.succeeded()) {
                                        context.response().setStatusCode(OK.code()).end();
                                } else {
                                        context.fail(res.cause());
                                }
                        }
                );
        }

        public void handleRead(final RoutingContext context) {
                final var id = context.pathParam("id");
                vertx.executeBlocking(
                        fut -> fut.complete(lsmService.getData(id)),
                        false,
                        res -> {
                                if (res.succeeded()) {
                                        final var data = Optional.ofNullable((String) res.result()).orElse("{}");
                                        context.response().putHeader("content-type", "application/json").end(data);
                                } else {
                                        log.error(res.cause().toString());
                                        context.response().setStatusCode(NOT_FOUND.code()).end();
                                }
                        }
                );
        }

}
