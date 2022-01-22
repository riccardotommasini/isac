package fr.liris.insa.isac;

import org.apache.jena.graph.Node;

public class TP {

    String predicate, subject, object;

    public String getPredicate() {
        return predicate;
    }

    public void setPredicate(String predicate) {
        this.predicate = predicate;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getObject() {
        return object;
    }

    public void setObject(String object) {
        this.object = object;
    }

    public TP(Node subject, Node predicate, Node object) {
        this.predicate = predicate.toString();
        this.subject = subject.toString();
        this.object = object.toString();
    }

    public TP(String subject, String predicate, String object) {
        this.predicate = predicate;
        this.subject = subject;
        this.object = object;
    }

    @Override
    public String toString() {
        return "<" +
               "'" + subject + '\'' +
               ",'" + predicate + '\'' +
               ", '" + object + '\'' +
               '>';
    }
}
