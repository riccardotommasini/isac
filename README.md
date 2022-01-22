# Streaming graph Isomorphism with Compiled Automata

Idea: compiling a graph query into a series of automaton that can execute the subgraph isomorphism in a streaming fashion.

Current implementation uses Apache Jena 4.3.2 and Esper from Espertech 8.8.0

Jena is used for parsing the SPARQL query (as an example of graph query)
Esper is used for implementing the NFA using EPL

## Build

```
mvn clean package

mv target/isac-1.0-SNAPSHOT.jar isac.jar      
```

## RUN

## Code Generation



```
java -cp isac.jar fr.liris.insa.isac.run.GenerateEPLQueries query.sparql queries.epl log.txt
```

#### Input SPARQL Query

```sparql
PREFIX dc: <http://purl.org/dc/terms/>
PREFIX foaf: <http://xmlns.com/foaf/>
PREFIX gr: <http://purl.org/goodrelations/>
PREFIX gn: <http://www.geonames.org/ontology#>
PREFIX mo: <http://purl.org/ontology/mo/>
PREFIX og: <http://ogp.me/ns#>
PREFIX rev: <http://purl.org/stuff/rev#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
PREFIX sorg: <http://schema.org/>

SELECT  ?v0 ?v2 ?v3
WHERE
  { ?v0  wsdbm:subscribes  ?v1 .
    ?v2  sorg:caption      ?v3 .
    ?v0  wsdbm:likes       ?v2
  }
```

#### Output EPL Queris & Log

```sql
@name('012') select distinct t1.object as v3, t1.subject as v2, t0.subject as v0 from pattern [every t0=MyTriple(predicate="subscribes") -> every t1=MyTriple(predicate="caption") -> t2=MyTriple(predicate="likes" and object=t1.subject)];
@name('021') select distinct t1.object as v3, t2.object as v2, t0.subject as v0 from pattern [every t0=MyTriple(predicate="subscribes") -> every t2=MyTriple(predicate="likes") -> t1=MyTriple(predicate="caption")];
@name('102') select distinct t1.object as v3, t1.subject as v2, t0.subject as v0 from pattern [every t1=MyTriple(predicate="caption") -> every t0=MyTriple(predicate="subscribes") -> t2=MyTriple(predicate="likes" and object=t1.subject)];
@name('120') select distinct t1.object as v3, t1.subject as v2, t2.subject as v0 from pattern [every t1=MyTriple(predicate="caption") -> every t2=MyTriple(predicate="likes" and object=t1.subject) -> t0=MyTriple(predicate="subscribes")];
@name('201') select distinct t1.object as v3, t2.object as v2, t2.subject as v0 from pattern [every t2=MyTriple(predicate="likes") -> every t0=MyTriple(predicate="subscribes") -> t1=MyTriple(predicate="caption")];
@name('210') select distinct t1.object as v3, t2.object as v2, t2.subject as v0 from pattern [every t2=MyTriple(predicate="likes") -> every t1=MyTriple(predicate="caption") -> t0=MyTriple(predicate="subscribes")];
```

```
0;1;2;
TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/subscribes ?v1, tagname='0'}
subject join map
{?v0=[TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/likes ?v2, tagname='2'}]}
object join map
{?v1=[]}
predicate join map
{http://db.uwaterloo.ca/~galuc/wsdbm/subscribes=[]}
---------
TripleEvent{triple=?v2 @http://schema.org/caption ?v3, tagname='1'}
subject join map
{?v2=[TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/likes ?v2, tagname='2'}]}
object join map
{?v3=[]}
predicate join map
{http://schema.org/caption=[]}
---------
TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/likes ?v2, tagname='2'}
subject join map
{?v0=[TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/subscribes ?v1, tagname='0'}]}
object join map
{?v2=[TripleEvent{triple=?v2 @http://schema.org/caption ?v3, tagname='1'}]}
predicate join map
{http://db.uwaterloo.ca/~galuc/wsdbm/likes=[]}
---------
0;2;1;
TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/subscribes ?v1, tagname='0'}
subject join map
{?v0=[TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/likes ?v2, tagname='2'}]}
object join map
{?v1=[]}
predicate join map
{http://db.uwaterloo.ca/~galuc/wsdbm/subscribes=[]}
---------
TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/likes ?v2, tagname='2'}
subject join map
{?v0=[TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/subscribes ?v1, tagname='0'}]}
object join map
{?v2=[TripleEvent{triple=?v2 @http://schema.org/caption ?v3, tagname='1'}]}
predicate join map
{http://db.uwaterloo.ca/~galuc/wsdbm/likes=[]}
---------
TripleEvent{triple=?v2 @http://schema.org/caption ?v3, tagname='1'}
subject join map
{?v2=[TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/likes ?v2, tagname='2'}]}
object join map
{?v3=[]}
predicate join map
{http://schema.org/caption=[]}
---------
1;0;2;
TripleEvent{triple=?v2 @http://schema.org/caption ?v3, tagname='1'}
subject join map
{?v2=[TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/likes ?v2, tagname='2'}]}
object join map
{?v3=[]}
predicate join map
{http://schema.org/caption=[]}
---------
TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/subscribes ?v1, tagname='0'}
subject join map
{?v0=[TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/likes ?v2, tagname='2'}]}
object join map
{?v1=[]}
predicate join map
{http://db.uwaterloo.ca/~galuc/wsdbm/subscribes=[]}
---------
TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/likes ?v2, tagname='2'}
subject join map
{?v0=[TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/subscribes ?v1, tagname='0'}]}
object join map
{?v2=[TripleEvent{triple=?v2 @http://schema.org/caption ?v3, tagname='1'}]}
predicate join map
{http://db.uwaterloo.ca/~galuc/wsdbm/likes=[]}
---------
1;2;0;
TripleEvent{triple=?v2 @http://schema.org/caption ?v3, tagname='1'}
subject join map
{?v2=[TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/likes ?v2, tagname='2'}]}
object join map
{?v3=[]}
predicate join map
{http://schema.org/caption=[]}
---------
TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/likes ?v2, tagname='2'}
subject join map
{?v0=[TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/subscribes ?v1, tagname='0'}]}
object join map
{?v2=[TripleEvent{triple=?v2 @http://schema.org/caption ?v3, tagname='1'}]}
predicate join map
{http://db.uwaterloo.ca/~galuc/wsdbm/likes=[]}
---------
TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/subscribes ?v1, tagname='0'}
subject join map
{?v0=[TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/likes ?v2, tagname='2'}]}
object join map
{?v1=[]}
predicate join map
{http://db.uwaterloo.ca/~galuc/wsdbm/subscribes=[]}
---------
2;0;1;
TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/likes ?v2, tagname='2'}
subject join map
{?v0=[TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/subscribes ?v1, tagname='0'}]}
object join map
{?v2=[TripleEvent{triple=?v2 @http://schema.org/caption ?v3, tagname='1'}]}
predicate join map
{http://db.uwaterloo.ca/~galuc/wsdbm/likes=[]}
---------
TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/subscribes ?v1, tagname='0'}
subject join map
{?v0=[TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/likes ?v2, tagname='2'}]}
object join map
{?v1=[]}
predicate join map
{http://db.uwaterloo.ca/~galuc/wsdbm/subscribes=[]}
---------
TripleEvent{triple=?v2 @http://schema.org/caption ?v3, tagname='1'}
subject join map
{?v2=[TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/likes ?v2, tagname='2'}]}
object join map
{?v3=[]}
predicate join map
{http://schema.org/caption=[]}
---------
2;1;0;
TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/likes ?v2, tagname='2'}
subject join map
{?v0=[TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/subscribes ?v1, tagname='0'}]}
object join map
{?v2=[TripleEvent{triple=?v2 @http://schema.org/caption ?v3, tagname='1'}]}
predicate join map
{http://db.uwaterloo.ca/~galuc/wsdbm/likes=[]}
---------
TripleEvent{triple=?v2 @http://schema.org/caption ?v3, tagname='1'}
subject join map
{?v2=[TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/likes ?v2, tagname='2'}]}
object join map
{?v3=[]}
predicate join map
{http://schema.org/caption=[]}
---------
TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/subscribes ?v1, tagname='0'}
subject join map
{?v0=[TripleEvent{triple=?v0 @http://db.uwaterloo.ca/~galuc/wsdbm/likes ?v2, tagname='2'}]}
object join map
{?v1=[]}
predicate join map
{http://db.uwaterloo.ca/~galuc/wsdbm/subscribes=[]}
---------

```

### ISAC Execution

```
java -jar isac.jar query.sparql queries.epl inputdata.nt output.csv
```

Result looks like this

```
stmt,v0,v2,v3
102,User0,Product14839,"sectarianism Sm's dashboards beachcomber's swamp's"
102,User0,Product14839,"sectarianism Sm's dashboards beachcomber's swamp's"
102,User0,Product14839,"sectarianism Sm's dashboards beachcomber's swamp's"
102,User0,Product14839,"sectarianism Sm's dashboards beachcomber's swamp's"
102,User0,Product14839,"sectarianism Sm's dashboards beachcomber's swamp's"
102,User0,Product14839,"sectarianism Sm's dashboards beachcomber's swamp's"
102,User0,Product14839,"sectarianism Sm's dashboards beachcomber's swamp's"
102,User10000,Product14839,"sectarianism Sm's dashboards beachcomber's swamp's"
102,User10000,Product14839,"sectarianism Sm's dashboards beachcomber's swamp's"
102,User10000,Product14839,"sectarianism Sm's dashboards beachcomber's swamp's"
...
```

Results contains also log information

Start of Load [xxx]
Start of Run [yyy]
Streamed 0 events
Streamed 100000 events
Streamed 200000 events
...
Streamed 1900000 events
Streamed 2000000 events
End of Run [zzz]

Progress is evey 100k triples streamed.

latencyLoad = yyy - xxx
latencyExecution = zzz - yyy