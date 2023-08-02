package com.vertx.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

public class TestVerticle extends AbstractVerticle {

        @Override
        public void start(Promise<Void> startPromise) {
                HttpServer server = vertx.createHttpServer();
                Router router = Router.router(vertx);
                router.route().handler(BodyHandler.create());
                router.post("/echo").handler(this::echoPayload);
                server.requestHandler(router);
                server.listen(8080, http -> {
                        if (http.succeeded()) {
                                System.out.println("Server started on port 8080");
                                startPromise.complete();
                        } else {
                                startPromise.fail(http.cause());
                        }
                });
        }

        private void echoPayload(RoutingContext routingContext) {
                String payload = routingContext.getBodyAsString();
                HttpServerResponse response = routingContext.response();
                response.putHeader("Content-Type", "text/plain");
                response.end(payload);
        }
}
