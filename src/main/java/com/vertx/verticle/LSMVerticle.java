package com.vertx.verticle;

import com.lsmdb.LSM;
import com.lsmdb.service.LSMService;
import com.vertx.AppConfig;
import com.vertx.http.HttpHandler;
import com.vertx.http.HttpRoutes;
import com.vertx.http.HttpServer;
import com.vertx.scheduling.SchedulerConfig;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.quartz.SchedulerException;

import java.util.Optional;

@Slf4j
public class LSMVerticle extends AbstractVerticle {

        @Override
        public void start(final Promise<Void> promise) {
                AppConfig.init(vertx)
                        .compose(this::boot)
                        .onSuccess(s -> promise.complete())
                        .onFailure(e -> promise.fail(e.getMessage()));
        }

        private Future<io.vertx.core.http.HttpServer> boot(final JsonObject config) {
                final LSMService lsmService = LSM.getLsmService();
                final var httpHandler = new HttpHandler(lsmService, vertx);
                setupScheduledMerging(config, lsmService);
                final var server = new HttpServer(vertx, new HttpRoutes(vertx, httpHandler).defineRoutes());
                Integer port = Optional.ofNullable(config.getJsonObject("http"))
                        .map(it -> it.getInteger("port"))
                        .orElse(8081);

                return server.initialiseAndSetup(port);
        }

        private void setupScheduledMerging(final JsonObject config, final LSMService lsmService) {
                try {
                        new SchedulerConfig().scheduleMergeSegments(config, lsmService);
                } catch (SchedulerException schedulerException) {
                        log.error("error occurred while setting up scheduling", schedulerException);
                }
        }

}
