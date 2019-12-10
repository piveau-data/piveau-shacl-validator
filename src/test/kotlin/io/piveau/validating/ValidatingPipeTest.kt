package io.piveau.validating

import io.piveau.MainVerticle
import io.piveau.pipe.Pipe
import io.piveau.utils.JenaUtils
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpMethod
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.HttpResponse
import io.vertx.ext.web.client.WebClient
import io.vertx.junit5.Checkpoint
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import org.apache.jena.riot.Lang
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension::class)
class ValidatingPipeTest {

    private val log = LoggerFactory.getLogger(this.javaClass)

    private var webClient: WebClient? = null

    @BeforeAll
    fun setup(vertx: Vertx, testContext: VertxTestContext) {
        webClient = WebClient.create(vertx)
        vertx.deployVerticle(
            MainVerticle::class.java.name,
            DeploymentOptions(),
            testContext.completing<String>()
        )
    }

    @Test
    fun `Send a pipe and check if it is forwarded`(vertx: Vertx, testContext: VertxTestContext) {
        val checkpoint = testContext.checkpoint(2)

        vertx.createHttpServer().requestHandler { request ->
            if (request.method() == HttpMethod.POST && request.path() == "/pipe") {
                request.bodyHandler { buffer ->
                    val (_, body) = Json.mapper.convertValue(buffer.toJsonObject(), Pipe::class.java)
                    val data = body.segments[1].body.payload!!.body.data
                    val dataset = JenaUtils.readDataset(data.toByteArray(), null)
                    log.debug(JenaUtils.write(dataset, Lang.TRIG))
                }
                request.response().setStatusCode(202).end {
                    if (it.succeeded()) {
                        checkpoint.flag()
                    } else {
                        testContext.failNow(it.cause())
                    }
                }
            } else {
                testContext.failNow(Throwable("Unknown request"))
            }
        }.listen(8098)

        sendPipe("test-pipe.json", vertx, testContext, checkpoint)
    }

    private fun sendPipe(pipeFile: String, vertx: Vertx, testContext: VertxTestContext, checkpoint: Checkpoint) {
        vertx.fileSystem().readFile("src/test/resources/$pipeFile") { result ->
            if (result.succeeded()) {
                val pipe = JsonObject(result.result())
                val client = WebClient.create(vertx)
                client.post(8080, "localhost", "/pipe")
                    .putHeader("Content-Type", "application/json")
                    .sendJsonObject(pipe, testContext.succeeding<HttpResponse<Buffer>> { response ->
                        if (response.statusCode() == 202) {
                            checkpoint.flag()
                        } else {
                            testContext.failNow(Throwable(response.statusMessage()))
                        }
                    })
            } else {
                testContext.failNow(result.cause())
            }
        }
    }

}