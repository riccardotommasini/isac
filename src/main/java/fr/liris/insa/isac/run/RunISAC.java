package fr.liris.insa.isac.run;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.module.Module;
import com.espertech.esper.common.client.module.ParseException;
import com.espertech.esper.common.client.util.EventTypeBusModifier;
import com.espertech.esper.common.internal.event.map.MapEventBean;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
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
    private static String test = "all";
    private static String sparqlQueryFilePath = res + "query.sparql";


    public static void main(String[] args) throws IOException, ParseException, EPCompileException, EPDeployException {


        String sparqlQuery = (args.length > 0 && args[0] != null) ? args[0] : sparqlQueryFilePath;
        test = (args.length > 1 && args[1] != null) ? args[1] : test;

        String DefaulteplQueries = res + "queries." + test + ".epl";
        String csvOutputFilePath = res + "output." + test + ".csv";
        String rdfInputFilePath = res + test + ".test.nt";

        String eplQueries = (args.length > 2 && args[2] != null) ? args[1] : DefaulteplQueries;
        String inputFile = (args.length > 3 && args[3] != null) ? args[2] : rdfInputFilePath;
        String outputFile = (args.length > 4 && args[4] != null) ? args[3] : csvOutputFilePath;
        String log = outputFile.replace(".csv", ".log");
        org.apache.jena.query.ARQ.init();

        Query query = QueryFactory.read(sparqlQuery);
        PrefixMapping prefixMapping = query.getPrefixMapping();

        PrintWriter pw = new PrintWriter(new File(outputFile));
        PrintWriter lw = new PrintWriter(new File(log));

        List<String> varlist = query.getProjectVars().stream().filter(Node::isVariable).map(Node::getName).sorted().collect(Collectors.toList());

        pw.println("stmt," + String.join(COMMA, varlist));
        long load = System.nanoTime();
        lw.println("Start of Load [" + load + "]");
        pw.flush();
        lw.flush();


        InputStream eplFile = new FileInputStream(eplQueries);
        if (eplFile == null) {
            throw new RuntimeException("Failed to find file [" + eplQueries
                                       +
                                       "] in classpath or relative to classpath");
        }


        // read module
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        Module module = compiler.readModule(eplFile, eplQueries);

        Configuration config = new Configuration();
        config.getRuntime().getExecution().setPrioritized(true);
        config.getRuntime().getThreading().setInternalTimerEnabled(false);
        config.getRuntime().getThreading().setThreadPoolOutbound(true);
        config.getRuntime().getThreading().setThreadPoolOutboundNumThreads(1 + module.getItems().size());
        config.getCompiler().getByteCode().setBusModifierEventType(EventTypeBusModifier.BUS);
        config.getCompiler().getByteCode().setAccessModifiersPublic();
        config.getCommon().addEventType(EVENT_TYPE, TP.class);

        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(config);
        runtime.initialize();

        EPDeploymentService deploymentService = runtime.getDeploymentService();

        CompilerArguments args1 = new CompilerArguments(config);
        EPCompiled compiled = compiler.compile(module, args1);

        // set deployment id to 'trivia'
        EPDeployment deployment = deploymentService.deploy(compiled, new DeploymentOptions().setDeploymentId(eplQueries));

        ISACListener listener = new ISACListener(varlist, pw);

        EPStatement[] statements = deployment.getStatements();
        Arrays.stream(statements).forEach(statement -> {
            statement.addListener(listener);
        });

        Map<String, String> mp = prefixMapping.getNsPrefixMap();

        Iterator<Triple> iter = AsyncParser.asyncParseTriples(inputFile);
        var ref = new Object() {
            int i = 0;
        };

        long start = System.nanoTime();
        lw.println("Start of Run [" + start + "]");
        lw.flush();

        iter.forEachRemaining(triple -> {

            if (ref.i % 100000 == 0) {
                lw.println("Streamed " + ref.i + " events");
                lw.flush();
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
            pw.flush();
            pw.flush();
            ref.i++;
        });


        long end = System.nanoTime();
        lw.println("End of Run [" + end + "]");
        lw.flush();
        pw.flush();
        lw.close();
        pw.close();


    }

    private static class ISACListener implements UpdateListener {


        private List<String> varlist;
        private PrintWriter pw;

        public ISACListener(List<String> varlist, PrintWriter pw) {
            this.varlist = varlist;
            this.pw = pw;
        }

        @Override
        public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement statement, EPRuntime runtime) {
            for (EventBean ev : newEvents) {
                MapEventBean ev2 = ((MapEventBean) ev);
                String line = statement.getName();
                for (String key : varlist) {
                    line = line + "," + ev2.get(key).toString();
                }
                pw.println(line);
                System.out.println(line);
                pw.flush();
            }
        }
    }
}
