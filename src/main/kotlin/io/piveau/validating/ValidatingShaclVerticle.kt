package io.piveau.validating

import io.piveau.pipe.connector.PipeContext
import io.piveau.rdf.*
import io.piveau.vocabularies.vocabulary.DQV
import io.piveau.vocabularies.vocabulary.PROV
import io.piveau.vocabularies.vocabulary.PV
import io.piveau.vocabularies.vocabulary.SHACL
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.Lang
import org.apache.jena.vocabulary.DCAT
import org.apache.jena.vocabulary.OA
import org.apache.jena.vocabulary.RDF
import org.slf4j.LoggerFactory
import java.nio.charset.StandardCharsets
import java.time.Instant
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

class ValidatingShaclVerticle : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(this.javaClass)

    @ExperimentalTime
    override suspend fun start() {
        vertx.eventBus().consumer<PipeContext>(ADDRESS_PIPE, this::handlePipe)
        vertx.eventBus().consumer<JsonObject>(ADDRESS_REPORT, this::handleReport)
    }

    companion object {
        const val ADDRESS_PIPE: String = "io.piveau.pipe.validating.shacl.queue"
        const val ADDRESS_REPORT: String = "io.piveau.report.validating.shacl.queue"
    }

    @ExperimentalTime
    private fun handlePipe(message: Message<PipeContext>) {
        val pipeContext = message.body()

        if (pipeContext.config.path("skip").asBoolean(false)) {
            pipeContext.log().debug("Data validation skipped: {}", pipeContext.dataInfo)
            pipeContext.pass(vertx);
            return;
        }

        val qualityMetadataContext = "${pipeContext.pipe.header.context?.asNormalized()}:${pipeContext.pipe.header.name}"
        val content =
            if (pipeContext.pipeManager.isBase64Payload) pipeContext.binaryData else pipeContext.stringData.toByteArray()

        val dataset = content.toDataset(Lang.TRIG)
        val model = ModelFactory.createDefaultModel().add(dataset.defaultModel)

        val resourceType = model.resourceType()
        val resource = model.listSubjectsWithProperty(RDF.type, resourceType).next();

        val measured = measureTimedValue {
            validateModel(model, resourceType)
        }
        log.debug("Validation duration: {} milliseconds", measured.duration.toLongMilliseconds())
        val report = measured.value

        val qualityAnnotation = report.createResource(DQV.QualityAnnotation)
        report.listSubjectsWithProperty(RDF.type, SHACL.ValidationReport).forEachRemaining {
            qualityAnnotation.addProperty(OA.hasBody, it)
        }
        qualityAnnotation.addProperty(DQV.inDimension, PV.interoperability)
        val reportTarget = qualityAnnotation.model.createResource(resource.uri)
        qualityAnnotation.addProperty(OA.hasTarget, reportTarget)
        qualityAnnotation.addProperty(PROV.generatedAtTime, report.createTypedLiteral(Instant.now(), XSDDatatype.XSDdateTime))
        report.add(reportTarget, DQV.hasQualityAnnotation, qualityAnnotation)

        if (dataset.containsNamedModel("urn:$qualityMetadataContext")) {
            dataset.getNamedModel("urn:$qualityMetadataContext").add(report)
        } else {
            val qualityMetadata = report.createResource("urn:$qualityMetadataContext", DQV.QualityMetadata)
            qualityMetadata.addProperty(PROV.generatedAtTime, report.createTypedLiteral(Instant.now(), XSDDatatype.XSDdateTime))
            dataset.addNamedModel("urn:$qualityMetadataContext", report)
        }

        pipeContext.log().info("Data validated ({}): {}", measured.duration.toLongMilliseconds(), pipeContext.dataInfo)
        pipeContext.log().debug("Data content: {}", dataset.asString())
        pipeContext.setResult(dataset.asString(), RDFMimeTypes.TRIG, pipeContext.dataInfo).forward(vertx)
    }

    @ExperimentalTime
    private fun handleReport(message: Message<JsonObject>) {
        val contentType = message.body().getString("contentType").asRdfLang()
        val content = message.body().getString("content")
        val model = content.toByteArray(StandardCharsets.UTF_8).toModel(contentType)

        val measured = measureTimedValue {
            validateModel(model, model.resourceType())
        }
        log.debug("Validation duration: {} milliseconds", measured.duration.toLongMilliseconds())
        message.reply(measured.value.asString(Lang.TURTLE))
    }

}

private fun Model.resourceType() = when {
    contains(null, RDF.type, DCAT.Catalog) -> DCAT.Catalog
    contains(null, RDF.type, DCAT.Dataset) -> DCAT.Dataset
    contains(null, RDF.type, DCAT.Distribution) -> DCAT.Distribution
    contains(null, RDF.type, DCAT.CatalogRecord) -> DCAT.CatalogRecord
    // fallback
    else -> DCAT.Dataset
}