package io.piveau.validating

import io.piveau.rdf.asString
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.Lang
import org.apache.jena.vocabulary.DCAT
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.system.measureTimeMillis

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ValidatingShaclTest {

    @Test
    fun `Validate a dataset 5 times and measure the durations`() = runBlocking<Unit> {
        ModelFactory.createDefaultModel().apply {
            readTurtleResource("test.ttl")
            repeat(5) {
                launch {
                    val milli = measureTimeMillis {
                        val report = validateModel(this@apply, DCAT.Dataset)
//                        println(report.asString(Lang.TURTLE))
                    }
                    println("Validation $it took $milli milliseconds")
                }
            }
        }
    }

}