package io.piveau.validating

import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.Lang
import org.apache.jena.riot.RDFDataMgr
import org.topbraid.shacl.arq.SHACLFunctions
import org.topbraid.shacl.engine.ShapesGraph
import org.topbraid.shacl.engine.filters.ExcludeMetaShapesFilter
import org.topbraid.shacl.util.SHACLSystemModel
import java.io.InputStream

object Vocabularies {
    val vocabularies = ModelFactory.createDefaultModel()!!
    val shapesGraph: ShapesGraph

    init {
        val dcatapShapes = ModelFactory.createDefaultModel()
        with(dcatapShapes) {
            readTurtleResource("rdf/dcat-ap_1.2.1_shacl_shapes.ttl")
            readTurtleResource("rdf/dcat-ap_1.2.1_shacl_mandatory-classes.shapes.ttl")
            readTurtleResource("rdf/dcat-ap_1.2.1_shacl_mdr-vocabularies.shape.ttl")
        }

        val unionShapes = SHACLSystemModel.getSHACLModel().union(dcatapShapes)

        SHACLFunctions.registerFunctions(unionShapes)
        shapesGraph = ShapesGraph(unionShapes)
        shapesGraph.setShapeFilter(ExcludeMetaShapesFilter())

        with(vocabularies) {
            readXmlResource("rdf/ADMS_SKOS_v1.00.rdf")
            readXmlResource("rdf/continents-skos.rdf")
            readXmlResource("rdf/corporatebodies-skos.rdf")
            readXmlResource("rdf/countries-skos.rdf")
            readXmlResource("rdf/data-theme-skos.rdf")
            readXmlResource("rdf/filetypes-skos.rdf")
            readXmlResource("rdf/frequencies-skos.rdf")
            readXmlResource("rdf/languages-skos.rdf")
            readXmlResource("rdf/places-skos.rdf")
        }
    }
}

fun String.loadResource(): InputStream? = ValidatingShaclVerticle::class.java.classLoader.getResourceAsStream(this)

fun Model.readTurtleResource(resource: String) = RDFDataMgr.read(this, resource.loadResource(), Lang.TURTLE)
fun Model.readXmlResource(resource: String) = RDFDataMgr.read(this, resource.loadResource(), Lang.RDFXML)