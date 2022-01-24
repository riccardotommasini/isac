/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.liris.insa.isac;

import com.espertech.esper.common.client.soda.*;
import com.google.common.collect.Collections2;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.util.NodeUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static fr.liris.insa.isac.Constants.*;
import static org.apache.commons.lang3.StringUtils.LF;

/**
 * Simple example to show parsing a query and producing the
 * SPARQL algebra expression for the query.
 */
public class ISACCompiler {


    private static List<String> epls = new ArrayList<>();


    public static void main(String[] args) throws Exception {

//        String s = "SELECT DISTINCT ?s { ?s <p> ?o , ?x . <s> ?o ?m . <s> <f> ?s }";
//        String s = "SELECT DISTINCT ?s { ?s <p ?o , ?x ; <q> ?m . <s> <f> ?s }";
//        String s = "PREFIX : <http://example.org/> \n" + "SELECT DISTINCT ?o { ?s <p> ?o . ?o ?p ?y .  }";
        String s =
                "PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/> \n" +
                "PREFIX sorg: <http://schema.org/> \n" +
                "SELECT ?v0 ?v2 ?v3 WHERE {\n" +
                " ?v0 wsdbm:subscribes ?v1 .\n" +
                " ?v2 sorg:caption ?v3 .\n" +
                " ?v0 wsdbm:likes ?v2 .\n" +
                "}";

//        String s =
//                "PREFIX dc: <http://purl.org/dc/terms/> \n" +
//                "PREFIX foaf: <http://xmlns.com/foaf/> \n" +
//                "PREFIX gr: <http://purl.org/goodrelations/> \n" +
//                "PREFIX gn: <http://www.geonames.org/ontology#> \n" +
//                "PREFIX mo: <http://purl.org/ontology/mo/> \n" +
//                "PREFIX og: <http://ogp.me/ns#> \n" +
//                "PREFIX rev: <http://purl.org/stuff/rev#> \n" +
//                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
//                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n" +
//                "PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/> \n" +
//                "PREFIX sorg: <http://schema.org/> \n" +
//                "SELECT * WHERE {\n" +
////                " ?v0 wsdbm:subscribes ?v1 .\n" +
//                " ?v0 sorg:eligibleRegion ?v1 .\n" +
////                " ?v2 sorg:caption ?v3 .\n" +
////                " ?v0 wsdbm:likes ?v2 .\n" +
//                "}";

//        String s = "SELECT ?subject \n" +
//                   "FROM NAMED <http://www.xyz.com/namespace/graph1>\n" +
//                   "FROM NAMED <http://www.xyz.com/namespace/graph2>\n" +
//                   "WHERE \n" +
//                   "{\n" +
//                   "  GRAPH <http://www.xyz.com/namespace/graph1> \n" +
//                   "  {\n" +
//                   "    ?subject <hasValue> \"23\" .\n" +
//                   "  }\n" +
//                   "  GRAPH <http://www.xyz.com/namespace/graph2> \n" +
//                   "  {\n" +
//                   "    ?subject <hasFeature> \"23\" .\n" +
//                   "  }\n" +
//                   "} ";

        Query query = QueryFactory.create(s, "http://example.org");

        PrefixMapping prefixMapping = query.getPrefixMapping();

        System.out.println(query);

        Op op = Algebra.compile(query);

        compile(op, prefixMapping, "/Users/rictomm/_Projects/jena-examples/src/main/resources/logs.txt");

    }

    public static ISACVisitor compile(Op op, PrefixMapping prefixMapping, String log) throws IOException {
        ISACVisitor opVisitor = new ISACVisitor();
        op.visit(opVisitor);

        HashMap<String, List<TPEvent>> permutations = new HashMap<>();

        for (List<TPEvent> p : Collections2.permutations(opVisitor.joins.keySet())) {
            permutations.put(p.stream().map(tripleEvent -> tripleEvent.tagname).map(integer -> integer + ";").collect(Collectors.joining()), p);
        }

        FileWriter logWriter = new FileWriter(log);

        List<Map.Entry<String, List<TPEvent>>> sorted = permutations.entrySet().stream().sorted(Map.Entry.comparingByKey()).collect(Collectors.toList());
        sorted.forEach(e -> {
            try {
                generateEPL(opVisitor, e.getKey(), e.getValue(), logWriter, prefixMapping);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        });


//        extracted(encoding, joins, "0;2;3;1;", permutations.get("0;2;3;1;"), queryWriter, logWriter);


        logWriter.close();


        return opVisitor;
    }

    private static void generateEPL(ISACVisitor visitor, String patternName, List<TPEvent> p, FileWriter logWriter, PrefixMapping prefixMapping) throws IOException {
        Map<TPEvent, Map<Node, Set<TPEvent>>[]> joins = visitor.joins;
        Map<Node, List<String>> projVars = visitor.projVars;
        logWriter.write(patternName + LF);

        EPStatementObjectModel stmt = new EPStatementObjectModel();

        stmt.setAnnotations(Arrays.asList(AnnotationPart.nameAnnotation(patternName.replace(SEMICOLON, StringUtils.EMPTY))));

        TPEvent te = p.get(0);

        PatternFilterExpr pat_head = Patterns.filter(EVENT_TYPE, eventID + te.tagname);

        for (int i = 0; i < te.nodes.length; i++) {
            Node n = te.nodes[i];
            if (!n.isVariable()) {
                pat_head.getFilter().setFilter(Expressions.eq(rdfTermsS[i], encoding(n.getURI(), prefixMapping)));
            } else {
                String propertyLeft = eventID + te.tagname + PERIOD + rdfTermsS[i];
                projVars.computeIfPresent(n, (node, strings) -> {
                    strings.add(propertyLeft);
                    return strings;
                });
            }
        }

        logWriter.write(te + LF);
        logWriter.write("subject join map" + LF);
        logWriter.write(joins.get(te)[SUBJECT].toString() + LF);
        logWriter.write("object join map" + LF);
        logWriter.write(joins.get(te)[OBJECT].toString() + LF);
        logWriter.write("predicate join map" + LF);
        logWriter.write(joins.get(te)[PREDICATE].toString() + LF);
        logWriter.write("---------" + LF);

        PatternExpr temp = Patterns.every(pat_head);

        List<Integer> tags = Arrays.stream(patternName.split(SEMICOLON)).map(Integer::valueOf).collect(Collectors.toList());

        for (int i = 1; i < p.size(); i++) {
            TPEvent key = p.get(i);
            int tag = key.tagname;
            Map<Node, Set<TPEvent>>[] maps = joins.get(key);

            PatternFilterExpr pat = Patterns.filter(EVENT_TYPE, eventID + tag);

            Filter filter = pat.getFilter();
            for (int j = 0; j < key.nodes.length; j++) {
                Expression expression = filter.getFilter();
                Node n = key.nodes[j];
                if (!n.isVariable()) {
                    if (expression == null)
                        filter.setFilter(Expressions.eq(rdfTermsS[j], encoding(n.getURI(), prefixMapping)));
                    else
                        filter.setFilter(Expressions.and(expression, Expressions.eq(rdfTermsS[j], encoding(n.getURI(), prefixMapping))));
                } else {
                    filter = setFilterProperty(n, pat, maps, rdfTermsS[j], tags, visitor.joins, visitor.projVars);
                }
            }

//            if (i == p.size() - 1)
//                temp = Patterns.followedBy(temp, pat);
//            else
            temp = Patterns.followedBy(temp, Patterns.every(pat));
//                temp = Patterns.followedBy(temp, pat);

            Map<Node, Set<TPEvent>>[] a = joins.get(key);

            logWriter.write(key + LF);
            logWriter.write("subject join map" + LF);
            logWriter.write(a[SUBJECT].toString() + LF);
            logWriter.write("object join map" + LF);
            logWriter.write(a[OBJECT].toString() + LF);
            logWriter.write("predicate join map" + LF);
            logWriter.write(a[PREDICATE].toString() + LF);
            logWriter.write("---------" + LF);
        }


        Stream<Optional<SelectClauseExpression>> optionalStream = projVars.entrySet().stream().map(entry -> {
            Optional<String> min = entry.getValue().stream().min((o, o2) -> {
                String[] keysO1 = o.substring(1).split("\\.");
                String[] keysO2 = o2.substring(1).split("\\.");
                Integer i = tags.indexOf(Integer.parseInt(keysO1[0]));
                Integer j = tags.indexOf(Integer.parseInt(keysO2[0]));
                int i1 = i.compareTo(j);
                return i1;
            });
            return min.map(s -> new SelectClauseExpression(new PropertyValueExpression(s), entry.getKey().getName()));
        });

        List<SelectClauseElement> collect = optionalStream.flatMap(Optional::stream).collect(Collectors.toList());

        SelectClause selectClause1 = SelectClause.create();

        selectClause1.distinct();

        selectClause1.addElements(collect);

        stmt.setSelectClause(selectClause1);

        stmt.setFromClause(FromClause.create(PatternStream.create(temp)));
        String s = stmt.toEPL();
        visitor.eplQueries.add(stmt);
        epls.add(s);
    }

    private static Filter setFilterProperty(Node n, PatternFilterExpr pat, Map<Node, Set<TPEvent>>[] maps, String property, List<Integer> tags, Map<TPEvent, Map<Node, Set<TPEvent>>[]> joins, Map<Node, List<String>> projVars) {

        Set<TPEvent> tripleEvents;
        Filter filter = pat.getFilter();
        String tagName = pat.getTagName();
        int curr = Integer.parseInt(tagName.replace(eventID, StringUtils.EMPTY));

        String propertyLeft = tagName + PERIOD + property;
        projVars.computeIfPresent(n, (node, strings) -> {
            strings.add(propertyLeft);
            return strings;
        });

        for (int rdfTerm : rdfTermsI) {
            tripleEvents = maps[rdfTerm].get(n);
            if (tripleEvents != null)
                tripleEvents.stream().min(Comparator.comparingInt(o -> tags.indexOf(o.tagname))).ifPresent(triple -> {
                    int i = tags.indexOf(triple.tagname);
                    int i1 = tags.indexOf(curr);
                    if (i < i1) {
                        Expression leftExpr = filter.getFilter();
                        for (String rdfTermS : rdfTermsS) {
                            if (NodeUtils.compareRDFTerms(n, triple.get(rdfTermS)) == Expr.CMP_EQUAL) {
                                String propertyRight = eventID + triple.tagname + PERIOD + rdfTermS;
                                RelationalOpExpression rightExpr = Expressions.eqProperty(property, propertyRight);
                                filter.setFilter((leftExpr != null) ? Expressions.and(leftExpr, rightExpr) : rightExpr);
                                projVars.computeIfPresent(n, (node, strings) -> {
                                    strings.add(propertyRight);
                                    return strings;
                                });
                            }
                        }
                    }
                });
        }
        return filter;
    }

    private static String encoding(String url, PrefixMapping prefixMapping) {
        String eurl = expandPrefixedName(url, prefixMapping);
        for (String s : prefixMapping.getNsPrefixMap().values()) {
            eurl = eurl.replace(s, StringUtils.EMPTY);
        }
        if (eurl == null) return url;
        return eurl;
    }

    public static String expandPrefixedName(String prefixed, PrefixMapping prefixMapping) {
        //From PrefixMappingImpl.expandPrefix( String prefixed )
        int colon = prefixed.indexOf(COLON);
        if (colon < 0) return prefixed;
        else {
            String prefix = prefixed.substring(0, colon);
            String uri = prefixMapping.getNsPrefixURI(prefix);
            if (uri == null) return prefixed;
            return uri + prefixed.substring(colon + 1);
        }
    }


}
