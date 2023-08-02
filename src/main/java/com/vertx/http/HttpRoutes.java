package com.vertx.http;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.DecodeException;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.micrometer.PrometheusScrapingHandler;
import lombok.extern.slf4j.Slf4j;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

@Slf4j
public class HttpRoutes {
  private final Vertx vertx;
  private final HttpHandler handler;

  public HttpRoutes(final Vertx vertx, final HttpHandler handler) {
    this.vertx = vertx;
    this.handler = handler;
  }

  public Router defineRoutes() {
    Router router = Router.router(vertx);
    router.route().handler(BodyHandler.create());

    addRoutes(router);

    return router;
  } 

  private void addRoutes(final Router router) {
    router.route("/*").failureHandler(handleServerErrors());

    router.route(HttpMethod.PUT, "/element/:id/timestamp/:timestamp")
        .handler(handler::handleUpdate);

    router.route(HttpMethod.GET, "/latest/element/:id")
        .handler(handler::handleRead);

    router.route("/metrics").handler(PrometheusScrapingHandler.create());
  }

  private Handler<RoutingContext> handleServerErrors() {
    return routingContext -> {
      log.error("Internal server error", routingContext.failure());
      if (routingContext.failure().getClass().equals(DecodeException.class)) {
        routingContext.response()
            .setStatusCode(BAD_REQUEST.code())
            .end(routingContext.failure().getMessage());
      } else {
        routingContext.response()
            .setStatusCode(routingContext.statusCode())
            .end("Internal server error");
      }
    };
  }
}
