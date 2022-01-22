package fr.liris.insa.isac;


import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import java.util.Objects;

import static fr.liris.insa.isac.Constants.*;

public class TPEvent {
    public Triple triple;
    public int tagname;
    public Node[] nodes;

    public Node get(int rdfTerm) {
        switch (rdfTerm) {
            case SUBJECT:
                return triple.getSubject();
            case OBJECT:
                return triple.getObject();
            case PREDICATE:
            default:
                return triple.getPredicate();
        }
    }

    public Node get(String rdfTerm) {
        switch (rdfTerm) {
            case SUBJECT_STRING:
                return triple.getSubject();
            case OBJECT_STRING:
                return triple.getObject();
            case PREDICATE_STRING:
            default:
                return triple.getPredicate();
        }
    }

    @Override
    public String toString() {
        return "TripleEvent{" + "triple=" + triple + ", tagname='" + tagname + '\'' + '}';
    }

    public TPEvent(Triple triple, int tagname) {
        this.triple = triple;
        this.tagname = tagname;
        this.nodes = new Node[]{triple.getSubject(), triple.getPredicate(), triple.getObject()};
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TPEvent that = (TPEvent) o;
        return Objects.equals(triple, that.triple) && Objects.equals(tagname, that.tagname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(triple, tagname);
    }
}