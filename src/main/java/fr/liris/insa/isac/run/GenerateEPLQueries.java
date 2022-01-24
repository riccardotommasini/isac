package fr.liris.insa.isac.run;

import fr.liris.insa.isac.ISACCompiler;
import fr.liris.insa.isac.ISACVisitor;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.algebra.Algebra;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.apache.commons.lang3.StringUtils.LF;

public class GenerateEPLQueries {

    private static final String res = "/Users/rictomm/_Projects/isac/src/main/resources/";
    private static final String log = res + "log.txt";
    private static final String defaulteplQueries = res + "queries.all.epl";
    private static final String sparqlQueryFilePath = res + "query.sparql";

    public static void main(String[] args) throws IOException {

        String sparqlQuery = (args.length > 0 && args[0] != null) ? args[0] : sparqlQueryFilePath;
        String eplQueries = (args.length > 0 && args[1] != null) ? args[1] : defaulteplQueries;
        String logFile = (args.length > 0 && args[2] != null) ? args[2] : log;

        org.apache.jena.query.ARQ.init();

        Query query = QueryFactory.read(sparqlQuery);
        PrefixMapping prefixMapping = query.getPrefixMapping();

        ISACVisitor compile = ISACCompiler.compile(Algebra.compile(query), prefixMapping, logFile);

        FileWriter queryWriter = new FileWriter(new File(eplQueries));

        compile.eplQueries.forEach(s -> {
            try {
                queryWriter.write(s.toEPL() + ";" + LF);
                queryWriter.flush();

                String replacement = s.getAnnotations().get(0).getAttributes().get(0).getValue().toString();
                FileWriter epl = new FileWriter(new File(eplQueries.replace(".all", "").replace("epl", replacement + ".epl")));
                FileWriter test = new FileWriter(new File(replacement + ".test.nt"));
                epl.write(s.toEPL());
                epl.flush();
                epl.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        queryWriter.close();
    }
}
