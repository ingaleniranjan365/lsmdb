package com.vertx.http;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MydbHttpServer {

  private final Vertx vertx;
  private final Router router;

  public MydbHttpServer(final Vertx vertx, final Router router) {
    this.vertx = vertx;
    this.router = router;
  }

  public Future<io.vertx.core.http.HttpServer> initialiseAndSetup(final Integer port) {
    return vertx.createHttpServer()
        .requestHandler(router)
        .listen(port)
        .onSuccess(server -> {
          log.info("Started http server on port - {}", port);
        })
        .onFailure(e -> {
          log.error("Fatal error! Failed to start server!");
        });
  }

}
