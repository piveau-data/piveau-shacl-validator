package io.piveau.validating

import io.piveau.vocabularies.vocabulary.SHACL
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.RDFNode
import org.apache.jena.rdf.model.Resource
import org.apache.jena.vocabulary.RDF
import org.topbraid.jenax.util.ARQFactory
import org.topbraid.shacl.validation.ValidationEngineConfiguration
import org.topbraid.shacl.validation.ValidationEngineFactory
import java.net.URI
import java.util.*

fun validateModel(model: Model, resourceType: Resource): Model {
    val dataset = ARQFactory.get().getDataset(model)
    model.add(Vocabularies.vocabularies)

    val shapesGraphURI = URI.create("urn:x-shacl-shapes-graph:" + UUID.randomUUID().toString())
    val engine = ValidationEngineFactory.get().create(dataset, shapesGraphURI, Vocabularies.shapesGraph, null)
    engine.configuration = ValidationEngineConfiguration().setValidateShapes(false);

    engine.applyEntailments()

    val nodes = ArrayList<RDFNode>()
    dataset.defaultModel.listResourcesWithProperty(RDF.type, resourceType).forEachRemaining { nodes.add(it) }

    val report = engine.validateNodesAgainstShape(nodes, resourceType.asNode())

    report.model.setNsPrefix("sh", SHACL.NS)

    return report.model
}
