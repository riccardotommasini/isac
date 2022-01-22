package fr.liris.insa.isac;


import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.soda.*;
import com.espertech.esper.common.internal.event.map.MapEventBean;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Var;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Created by riccardo on 04/09/2017.
 */
public class EPLFactory {

    public static EPStatementObjectModel toEPL(View window, List<AnnotationPart> annotations) {

        EPStatementObjectModel stmt = new EPStatementObjectModel();

        MatchRecognizeClause matchRecognizeClause = new MatchRecognizeClause();
        MatchRecognizeDefine matchRecognizeDefine = new MatchRecognizeDefine();
        MatchRecognizeIntervalClause intervalClause = new MatchRecognizeIntervalClause();


        stmt.setAnnotations(annotations);
        SelectClause selectClause1 = SelectClause.createWildcard(StreamSelector.RSTREAM_ISTREAM_BOTH);
        stmt.setSelectClause(selectClause1);

        SelectClause selectClause = selectClause1.addWildcard();
        OutputLimitClause outputLimitClause;
        OutputLimitSelector snapshot = OutputLimitSelector.ALL;
        TimePeriodExpression timePeriod = null;

        outputLimitClause = OutputLimitClause.create(snapshot, timePeriod);

//        Patterns.followedBy();


        stmt.setOutputLimitClause(outputLimitClause);
        FromClause fromClause = FromClause.create();
        Filter o = null;
        FilterStream stream = FilterStream.create(o);
        stream.addView(window);
        fromClause.add(stream);
        stmt.setFromClause(fromClause);

        //SETTING TICK
        OutputLimitUnit events = OutputLimitUnit.EVENTS;

        timePeriod = Expressions.timePeriod(null, null, null, null, null);
        outputLimitClause = OutputLimitClause.create(snapshot, timePeriod);
        stmt.setOutputLimitClause(outputLimitClause);

        return stmt;
    }


    public static List<AnnotationPart> getAnnotations(String name1, int range1, int step1, String s) {
        AnnotationPart name = new AnnotationPart();
        name.setName("Name");
        name.addValue(null);

        AnnotationPart range = new AnnotationPart();
        range.setName("Tag");
        range.addValue("name", "range");
        range.addValue("value", range1 + "");

        AnnotationPart slide = new AnnotationPart();
        slide.setName("Tag");
        slide.addValue("name", "step");
        slide.addValue("value", step1 + "");

        AnnotationPart stream_uri = new AnnotationPart();
        stream_uri.setName("Tag");
        stream_uri.addValue("name", "stream");
        stream_uri.addValue("value", null);

        return Arrays.asList(name, stream_uri, range, slide);
    }


    public static View getWindow(long range, String unitRange) {
        View view;
        ArrayList<Expression> parameters = new ArrayList<>();
        parameters.add(Expressions.constant(range));
        view = View.create("win", "length", parameters);
        parameters.add(getTimePeriod((int) range, unitRange));
        view = View.create("win", "time", parameters);
        return view;
    }

    private static TimePeriodExpression getTimePeriod(Integer omega, String unit_omega) {
        String unit = unit_omega.toLowerCase();
        if ("ms".equals(unit) || "millis".equals(unit) || "milliseconds".equals(unit)) {
            return Expressions.timePeriod(null, null, null, null, omega);
        } else if ("s".equals(unit) || "seconds".equals(unit) || "sec".equals(unit)) {
            return Expressions.timePeriod(null, null, null, omega, null);
        } else if ("m".equals(unit) || "minutes".equals(unit) || "min".equals(unit)) {
            return Expressions.timePeriod(null, null, omega, null, null);
        } else if ("h".equals(unit) || "hours".equals(unit) || "hour".equals(unit)) {
            return Expressions.timePeriod(null, omega, null, null, null);
        } else if ("d".equals(unit) || "days".equals(unit)) {
            return Expressions.timePeriod(omega, null, null, null, null);
        }
        return null;
    }

    public static String toEPLSchema(String s) {
        CreateSchemaClause schema = new CreateSchemaClause();
        schema.setSchemaName(s);
        schema.setInherits(new HashSet<String>(Arrays.asList(new String[]{"TStream"})));
        List<SchemaColumnDesc> columns = Arrays.asList(
                new SchemaColumnDesc("sys_timestamp", "long"),
                new SchemaColumnDesc("app_timestamp", "long"),
                new SchemaColumnDesc("content", Object.class.getTypeName()));
        schema.setColumns(columns);
        StringWriter writer = new StringWriter();
        schema.toEPL(writer);
        return writer.toString();
    }

    public static void main(String[] args) {
        EPStatementObjectModel stmt = new EPStatementObjectModel();
        SelectClause selectClause1 = SelectClause.createWildcard();
        stmt.setSelectClause(selectClause1);
        PatternFilterExpr a = Patterns.filter("MyTriple", "a");
        Node q = NodeFactory.createURI("q");
        a.getFilter().setFilter(Expressions.eq("predicate", q.getURI()));
        PatternFilterExpr b = Patterns.filter("MyTriple", "b");
        b.getFilter().setFilter(Expressions.eqProperty("subject", "a.subject"));
        PatternFilterExpr c = Patterns.filter("MyTriple", "c");
        PatternOrExpr bc = Patterns.or(b, c);
        PatternExpr pattern = Patterns.followedBy(Patterns.every(a), Patterns.every(b));

        stmt.setFromClause(FromClause.create(PatternStream.create(pattern)));
        System.out.println(stmt.toEPL());

        Configuration configuration = new Configuration();
        configuration.getCommon().addEventType(TP.class);

        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(configuration);

        CompilerArguments arg = new CompilerArguments(configuration);

        EPCompiler compiler = EPCompilerProvider.getCompiler();

        EPCompiled epCompiled;
        try {
            epCompiled = compiler.compile("@name('my-statement') " + stmt.toEPL(), arg);
        } catch (EPCompileException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }

        EPDeployment deployment;
        try {
            deployment = runtime.getDeploymentService().deploy(epCompiled);
        } catch (EPDeployException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }

        EPStatement statement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "my-statement");

        statement.addListener((newData, oldData, s, r) -> {
            for (EventBean ev : newData) {
                MapEventBean ev2 = ((MapEventBean) ev);
                System.out.println(ev2.get("a"));
                System.out.println(ev2.get("b"));
                System.out.println("---");
            }
        });


        Var var_x = Var.alloc("x");
        Var var_z = Var.alloc("z");

        // ---- Build expression

        runtime.getEventService().sendEventBean(new TP(NodeFactory.createURI("s"), NodeFactory.createURI("p"), NodeFactory.createURI("c")), "MyTriple");
        runtime.getEventService().sendEventBean(new TP(NodeFactory.createURI("s1"), NodeFactory.createURI("q"), NodeFactory.createURI("o")), "MyTriple");
        runtime.getEventService().sendEventBean(new TP(NodeFactory.createURI("s1"), NodeFactory.createURI("p"), NodeFactory.createURI("e")), "MyTriple");
        runtime.getEventService().sendEventBean(new TP(NodeFactory.createURI("s1"), NodeFactory.createURI("q"), NodeFactory.createURI("f")), "MyTriple");
        runtime.getEventService().sendEventBean(new TP(NodeFactory.createURI("s"), NodeFactory.createURI("q"), NodeFactory.createURI("c")), "MyTriple");

    }


}
