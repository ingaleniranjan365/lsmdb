package com.vertx;

import com.vertx.verticle.LSMVerticle;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MainVerticle {

        public static void main(final String[] args) {
                Vertx vertex = Vertx.vertx();

                var deployment = vertex.deployVerticle(new LSMVerticle());
                deployment.onFailure(it -> {
                        log.error("Unable to deploy LSM verticle", deployment.cause());
                        Runtime.getRuntime().exit(-1);
                });

        }
}
