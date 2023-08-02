package com.vertx;

import io.vertx.config.ConfigRetriever;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public class AppConfig {
  private AppConfig() {
  }

  public static Future<JsonObject> init(final Vertx vertx) {
    ConfigRetriever retriever = ConfigRetriever.create(vertx);
    return retriever.getConfig();
  }
}
