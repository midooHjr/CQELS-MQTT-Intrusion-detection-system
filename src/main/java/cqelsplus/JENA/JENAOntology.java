package cqelsplus.JENA;

import org.apache.jena.ontology.OntModel;
import org.apache.jena.rdf.model.*;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.util.FileManager;

import java.io.InputStream;

public class JENAOntology {

    private Model ontology ;

    public JENAOntology(){
        ontology = ModelFactory.createDefaultModel();
    }

    // read ontology from file 'filename'
    public JENAOntology(String filename){
        InputStream in = FileManager.get().open(filename);

        if (in == null)
            throw new IllegalArgumentException("File "+filename+" non-existent");

        OntModel model = ModelFactory.createOntologyModel();
        model.read(in,null);
        ontology = model;

    }

    public void addStatementToDataSet(String[] data){
        for (String aData : data) {
            String[] element = aData.split(" ");
            if (element.length == 3) {

                Resource subject = ontology.getResource(data[0]);
                if (subject == null) {
                    System.out.println("Wrong statement, subject unknown in ontology");
                    return;
                }

                Property predicate = ontology.getProperty(data[1]);
                if (predicate == null) {
                    System.out.println("Wrong statement, predicate unknown in ontology");
                    return;
                }

                Resource object = ontology.getResource(data[2]);
                if (object == null) {
                    System.out.println("Wrong statement, object unknown in ontology");
                    return;
                }

                ontology.add(ontology.createStatement(subject, predicate, object));

                //System.out.println("New data registered to ontology :\n"+subject.getProperty(predicate).toString());

            } else
                System.out.println("Error in message triple format (s,p,o)");
        }

    }

    public Model inferModel(Reasoner reasoner){
        if (ontology != null) {
            return ModelFactory.createInfModel(reasoner,ontology);
        }
        return null;
    }


    public Model getOntology() {
        return ontology;
    }

    public void setOntology(Model ontology) {
        this.ontology = ontology;
    }



}