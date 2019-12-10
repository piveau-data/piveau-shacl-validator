package io.piveau

import io.piveau.pipe.connector.PipeConnector
import io.piveau.validating.ValidatingShaclVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Launcher
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.api.contract.openapi3.OpenAPI3RouterFactory
import io.vertx.ext.web.handler.CorsHandler
import io.vertx.ext.web.handler.StaticHandler
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.awaitResult

class MainVerticle : CoroutineVerticle() {

    override suspend fun start() {
        awaitResult<String> { vertx.deployVerticle(ValidatingShaclVerticle::class.java, DeploymentOptions()
            .setWorker(true)
            .setInstances(16), it) }
        awaitResult<PipeConnector> { PipeConnector.create(vertx, it) }.apply {
            consumerAddress(ValidatingShaclVerticle.ADDRESS_PIPE)
            OpenAPI3RouterFactory.create(vertx, "webroot/openapi.yaml") {
                if (it.succeeded()) {
                    it.result().addHandlerByOperationId("validationReport", this@MainVerticle::handleValidation)
                    val router = it.result().router
                    router.route().order(0).handler(CorsHandler.create("*").allowedMethods(setOf(HttpMethod.POST)))
                    router.route("/*").handler(StaticHandler.create())

                    subRouter("/validation", it.result().router)
                } else {
                    throw it.cause()
                }
            }
        }
    }

    private fun handleValidation(context: RoutingContext) {
        val message = JsonObject()
            .put("contentType", context.request().getHeader("Content-Type"))
            .put("content", context.bodyAsString)
        vertx.eventBus().request<String>(ValidatingShaclVerticle.ADDRESS_REPORT, message) {
            if (it.succeeded()) {
                context.response().putHeader("Content-Type", "text/turtle").end(it.result().body())
            } else {
                context.response().setStatusCode(500).end()
            }
        }
    }

}

fun main(args: Array<String>) {
    Launcher.executeCommand("run", *(args.plus(MainVerticle::class.java.name)))
}
