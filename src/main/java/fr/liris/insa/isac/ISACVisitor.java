package fr.liris.insa.isac;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;

import java.util.*;
import java.util.stream.Stream;

public class ISACVisitor implements OpVisitor {

    public Map<TPEvent, Map<Node, Set<TPEvent>>[]> joins = new HashMap<>();
    public Map<Node, List<String>> projVars = new HashMap<>();
    public List<String> eplQueries = new ArrayList<>();
    private boolean wildcard = true;

    public void visit(OpBGP op1) {

        BasicPattern pattern = op1.getPattern();
        List<Triple> list = pattern.getList();

        //initialize join map
        list.forEach(triple -> {

            Map<Node, Set<TPEvent>> joinsS = new HashMap<>();
            joinsS.put(triple.getSubject(), new HashSet<>());
            Map<Node, Set<TPEvent>> joinsP = new HashMap<>();
            joinsP.put(triple.getPredicate(), new HashSet());
            Map<Node, Set<TPEvent>> joinsO = new HashMap<>();
            joinsO.put(triple.getObject(), new HashSet<>());

            joins.putIfAbsent(new TPEvent(triple, list.indexOf(triple)), new Map[]{joinsS, joinsP, joinsO});


        });


        //populate join map
        joins.entrySet().forEach(e -> {

            TPEvent te = e.getKey();
            Triple triple = te.triple;
            Map<Node, Set<TPEvent>>[] maps = e.getValue();
            Node[] nodes = new Node[]{triple.getSubject(), triple.getPredicate(), triple.getObject()};

            list.stream().filter(t -> !t.equals(triple)).forEach(tt -> {
                for (int i = 0; i < nodes.length; i++) {
                    Node n = nodes[i];
                    if (n.isVariable()) {
                        TPEvent tte = new TPEvent(tt, list.indexOf(tt));
                        boolean b = tt.getPredicate().isVariable() && n.getName().equals(tt.getPredicate().getName());
                        boolean b1 = tt.getSubject().isVariable() && n.getName().equals(tt.getSubject().getName());
                        boolean b2 = tt.getObject().isVariable() && n.getName().equals(tt.getObject().getName());
                        if (b || b1 || b2) {
                            maps[i].get(n).add(tte);
                        }
                    }
                }
            });

            if (wildcard) {
                list.stream()
                        .flatMap(t -> Stream.of(t.getSubject(), t.getPredicate(), t.getObject()))
                        .filter(Node::isVariable)
                        .forEach(node -> projVars.computeIfAbsent(node, n -> new ArrayList<>()));
            }
        });


    }

    @Override
    public void visit(OpQuadPattern op1) {
        System.out.println(op1);
    }

    @Override
    public void visit(OpQuadBlock op1) {
        System.out.println(op1);
    }

    @Override
    public void visit(OpTriple op1) {
    }

    @Override
    public void visit(OpQuad op1) {
        System.out.println(op1);
    }

    @Override
    public void visit(OpPath op1) {
    }

    @Override
    public void visit(OpTable op1) {

    }

    @Override
    public void visit(OpNull op1) {
    }

    @Override
    public void visit(OpProcedure op1) {
        op1.getSubOp().visit(this);

    }

    @Override
    public void visit(OpPropFunc op1) {
        op1.getSubOp().visit(this);
    }

    @Override
    public void visit(OpFilter op1) {
        op1.getSubOp().visit(this);
    }

    @Override
    public void visit(OpGraph op1) {
        op1.getSubOp().visit(this);
    }

    @Override
    public void visit(OpService op1) {
        op1.getSubOp().visit(this);
    }

    @Override
    public void visit(OpDatasetNames op1) {

    }

    @Override
    public void visit(OpLabel op1) {
        op1.getSubOp().visit(this);
    }

    @Override
    public void visit(OpAssign op1) {
        op1.getSubOp().visit(this);
    }

    @Override
    public void visit(OpExtend op1) {
        op1.getSubOp().visit(this);
    }

    @Override
    public void visit(OpJoin op1) {
        op1.getLeft().visit(this);
        op1.getRight().visit(this);

    }

    @Override
    public void visit(OpLeftJoin op1) {
        op1.getLeft().visit(this);
        op1.getRight().visit(this);
    }

    @Override
    public void visit(OpUnion op1) {
        op1.getLeft().visit(this);
        op1.getRight().visit(this);
    }

    @Override
    public void visit(OpDiff op1) {
        op1.getLeft().visit(this);
        op1.getRight().visit(this);
    }

    @Override
    public void visit(OpMinus op1) {
        op1.getLeft().visit(this);
        op1.getRight().visit(this);
    }

    @Override
    public void visit(OpConditional op1) {
        op1.getLeft().visit(this);
        op1.getRight().visit(this);
    }

    @Override
    public void visit(OpSequence op1) {
        op1.getElements().forEach(o -> o.visit(this));
    }

    @Override
    public void visit(OpDisjunction op1) {
        op1.getElements().forEach(o -> o.visit(this));
    }

    @Override
    public void visit(OpList op1) {
        op1.getSubOp().visit(this);
    }

    @Override
    public void visit(OpOrder op1) {
        op1.getSubOp().visit(this);
    }

    @Override
    public void visit(OpProject op1) {
        wildcard = false;
        op1.getVars().forEach(var -> projVars.put(var, new ArrayList<>()));
        op1.getSubOp().visit(this);
    }

    @Override
    public void visit(OpReduced op1) {
        op1.getSubOp().visit(this);
    }

    @Override
    public void visit(OpDistinct op1) {
        op1.getSubOp().visit(this);
    }

    @Override
    public void visit(OpSlice op1) {
        op1.getSubOp().visit(this);

    }

    @Override
    public void visit(OpGroup op1) {
        op1.getSubOp().visit(this);
    }

    @Override
    public void visit(OpTopN op1) {
        op1.getSubOp().visit(this);
    }
}