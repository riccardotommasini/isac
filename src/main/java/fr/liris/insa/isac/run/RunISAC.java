package fr.liris.insa.isac.run;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.module.Module;
import com.espertech.esper.common.client.util.EventTypeBusModifier;
import com.espertech.esper.common.internal.event.map.MapEventBean;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
import fr.liris.insa.isac.TP;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.jena.shared.PrefixMapping;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static fr.liris.insa.isac.Constants.COMMA;
import static fr.liris.insa.isac.Constants.EVENT_TYPE;
import static org.apache.commons.lang3.StringUtils.EMPTY;

public class RunISAC {

    private static final String res = "/Users/rictomm/_Projects/isac/src/main/resources/";
    private static final String DefaulteplQueries = res + "queries.epl";
    private static final String csvOutputFilePath = res + "output.csv";
    private static final String rdfInputFilePath = res + "data.nt";
    private static final String sparqlQueryFilePath = res + "query.sparql";

    public static void main(String[] args) throws IOException {

        String sparqlQuery = (args.length > 0 && args[0] != null) ? args[0] : sparqlQueryFilePath;
        String eplQueries = (args.length > 0 && args[1] != null) ? args[1] : DefaulteplQueries;
        String inputFile = (args.length > 0 && args[2] != null) ? args[2] : rdfInputFilePath;
        String outputFile = (args.length > 0 && args[3] != null) ? args[3] : csvOutputFilePath;

        org.apache.jena.query.ARQ.init();

        Query query = QueryFactory.read(sparqlQuery);
        PrefixMapping prefixMapping = query.getPrefixMapping();

        File csvOutputFile = new File(outputFile);
        PrintWriter pw = new PrintWriter(csvOutputFile);

        List<String> varlist = query.getProjectVars().stream().filter(Node::isVariable).map(Node::getName).sorted().collect(Collectors.toList());

        pw.println("stmt," + String.join(COMMA, varlist));
        long load = System.nanoTime();
        pw.println("Start of Load [" + load + "]");
        pw.flush();

        Configuration config = new Configuration();
        config.getRuntime().getExecution().setPrioritized(true);
        config.getRuntime().getThreading().setInternalTimerEnabled(false);
        config.getRuntime().getThreading().setThreadPoolOutbound(true);
        config.getCompiler().getByteCode().setBusModifierEventType(EventTypeBusModifier.BUS);
        config.getCompiler().getByteCode().setAccessModifiersPublic();
        config.getCommon().addEventType(EVENT_TYPE, TP.class);

        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(config);
        runtime.initialize();

        EPDeploymentService deploymentService = runtime.getDeploymentService();

        InputStream eplFile = new FileInputStream(eplQueries);
        if (eplFile == null) {
            throw new RuntimeException("Failed to find file [" + eplQueries
                                       +
                                       "] in classpath or relative to classpath");
        }

        try {


            // read module
            EPCompiler compiler = EPCompilerProvider.getCompiler();
            Module module = compiler.readModule(eplFile, eplQueries);

            CompilerArguments args1 = new CompilerArguments(config);
            EPCompiled compiled = compiler.compile(module, args1);

            // set deployment id to 'trivia'
            EPDeployment deployment = deploymentService.deploy(compiled, new DeploymentOptions().setDeploymentId("trivia"));

            EPStatement[] statements = deployment.getStatements();
            Arrays.stream(statements).forEach(statement -> {


                statement.addListener((newData, oldData, s, r) -> {

                    for (EventBean ev : newData) {
                        MapEventBean ev2 = ((MapEventBean) ev);
                        String line = s.getName();
                        for (String key : varlist) {
                            line = line + "," + ev2.get(key).toString();
                        }
                        pw.println(line);
                        pw.flush();
                    }
                });

            });

        } catch (Exception e) {
            throw new RuntimeException("Error compiling and deploying EPL from '" +
                                       eplQueries +
                                       "': " + e.getMessage(), e);
        }


        Map<String, String> mp = prefixMapping.getNsPrefixMap();

        Iterator<Triple> iter = AsyncParser.asyncParseTriples(inputFile);
        var ref = new Object() {
            int i = 0;
        };

        long start = System.nanoTime();
        pw.println("Start of Run [" + start + "]");

        iter.forEachRemaining(triple -> {

            if (ref.i % 100000 == 0) {
                pw.println("Streamed " + ref.i + " events");
            }
            //encoding
            String subject = triple.getSubject().toString();
            String predicate = triple.getPredicate().toString();
            String object = triple.getObject().toString();

            for (String prefix : mp.values()) {
                subject = subject.replace(prefix, EMPTY);
                object = object.replace(prefix, EMPTY);
                predicate = predicate.replace(prefix, EMPTY);
            }

            TP o = new TP(subject, predicate, object);

            runtime.getEventService().sendEventBean(o, EVENT_TYPE);

            ref.i++;
        });
        long end = System.nanoTime();
        pw.println("End of Run [" + end + "]");
    }
}
