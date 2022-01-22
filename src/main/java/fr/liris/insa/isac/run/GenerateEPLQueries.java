package fr.liris.insa.isac.run;

import fr.liris.insa.isac.ISACCompiler;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.algebra.Algebra;

import java.io.IOException;

public class GenerateEPLQueries {

    private static final String res = "/Users/rictomm/_Projects/isac/src/main/resources/";
    private static final String log = res + "log.txt";
    private static final String defaulteplQueries = res + "queries.epl";
    private static final String sparqlQueryFilePath = res + "query.sparql";

    public static void main(String[] args) throws IOException {

        String sparqlQuery = (args.length > 0 && args[0] != null) ? args[0] : sparqlQueryFilePath;
        String eplQueries = (args.length > 0 && args[1] != null) ? args[1] : defaulteplQueries;
        String logFile = (args.length > 0 && args[2] != null) ? args[2] : log;

        Query query = QueryFactory.read(sparqlQuery);
        PrefixMapping prefixMapping = query.getPrefixMapping();

        ISACCompiler.compile(Algebra.compile(query), prefixMapping, eplQueries, logFile);

    }
}
