package fr.liris.insa.isac.run;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.RDFDataMgr;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

public class RunJena {
    private static final String res = "/Users/rictomm/_Projects/isac/src/main/resources/";
    private static final String csvOutputFilePath = res + "output.jena.csv";
    private static final String rdfInputFilePath = res + "small.nt";
    private static final String sparqlQueryFilePath = res + "query.sparql";

    public static void main(String[] args) throws IOException {

        String sparqlQuery = (args.length > 0 && args[0] != null) ? args[0] : sparqlQueryFilePath;
        String input = (args.length > 0 && args[1] != null) ? args[1] : rdfInputFilePath;
        String outputFile = (args.length > 0 && args[2] != null) ? args[2] : csvOutputFilePath;

        org.apache.jena.query.ARQ.init();

        Query query = QueryFactory.read(sparqlQuery);
        File csvOutputFile = new File(outputFile);
        PrintWriter pw = new PrintWriter(csvOutputFile);

        long start = System.nanoTime();
        pw.println("Start of Run [" + start + "]");

        ResultSet resultSet = QueryExecutionFactory.create(query, RDFDataMgr.loadModel(input)).execSelect();
        while (resultSet.hasNext()) {
            pw.println(resultSet.next());
            pw.flush();
        }
        long end = System.nanoTime();
        pw.println("End of Run [" + end + "]");

    }
}
