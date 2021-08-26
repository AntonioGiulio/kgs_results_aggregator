const lodcloud_querier = require('lodcloud-querier');
const datahub_querier = require('datahub-querier');
const graphBuilder = require('ngraph.graph');
const pagerank = require('ngraph.pagerank');
const centrality = require('ngraph.centrality');
const fs = require('fs');

class KGs_Results_Aggregator {
    
    constructor() {
        this.lc_querier = new lodcloud_querier();
        this.dh_querier = new datahub_querier();
    }


    
    //PARALLEL
    brutalSearch(target){

        var lc_results = lcToStandard(this.lc_querier.brutalSearch(target));
        var dh_results = dhToStandard(this.dh_querier.brutalSearch(target));

        var mergedResults = JSON.parse('[]');
        var i = 0;
        
        //adesso dobbiamo mergiarli in maniera che non ci siano doppi
        
        //creo un grafo con gli id dei risultati da DataHub
        var dh_graph = graphBuilder();
        for(var kg in dh_results){
            dh_graph.addNode(dh_results[kg]['id']);
        }
        
        /*
        //stampiamo tutti i nodi 
        dh_graph.forEachNode(function(node){
            console.log(node.id);
        });*/

        //inizia la fase di confronto vera e propria
        for(var kg in lc_results){
            //se nell'insieme dei risultati di dh non è presente il risultato corrente di lod cloud lo aggiungo ai risultati finali, tutti gli altri saranno già parte dei risultati di DataHub
            if(dh_graph.getNode(lc_results[kg]['id']) == null){
                mergedResults[i++] = lc_results[kg];
            }
        }

        //aggiungo anche tutti i risultati di dh hub;
        for(var kg in dh_results){
            mergedResults[i++] = dh_results[kg];
        }      
        
        /*
        //temporaneo per analizzare i risultati, andrà eliminato 
        fs.writeFile("first_approach.json", JSON.stringify(mergedResults, null, 2), function(err) {
            if(err) {
                return console.log(err);
            }
            console.log("The file was saved!");
        }); */
        


        // @@@@@ dobbiamo fare qualcosa riguardo l'ordinamento; che è andato un po' a puttane 
        return this.generalSorting(mergedResults, arguments[1]);
    }


    multiTagSearch(target, ...tags){
        var brutalResults = this.brutalSearch(target, arguments[arguments.length-1]);
        var results = JSON.parse('[]');

        var field, i = 0;
        const pattern = new RegExp(target, 'i');
        for(var kg in brutalResults){
            field = '';
            for(var j in tags){
                field += JSON.stringify(brutalResults[kg][tags[j]]);
            }
            if(pattern.test(field)){
                results[i++] = brutalResults[kg];
            }
        }

        /*
        fs.writeFile("multiResults.json", JSON.stringify(results, null, 2), function(err) {
            if(err) {
                return console.log(err);
            }
            console.log("The file was saved!");
          }); */
            
        return results;
    }

    generalSorting(results, mode){
        switch(mode){
            case 'size':
                return this.sortResultsBySize(results);
                break;

            case 'name':
                return this.sortResultsByName(results)                ;
                break;

            case 'authority':
                return this.sortResultsByAuthority(results)                ;
                break;

            case 'centrality':
                return this.sortResultsByCentrality(results);
                break;

            default:
                return this.sortResultsByName(results);                
        }
    }

    sortResultsBySize(results){
        console.log('size ranking');
        results.sort(function(a, b){ return b.triples - a.triples});

        return results;
    }

    sortResultsByName(results){
        console.log('alphabetuc ranking');
        results.sort(function(a, b){
            var x = a.id.toLowerCase();
            var y = b.id.toLowerCase();
            if(x < y) {return -1;}
            if(x > y) {return 1;}
            return 0;
        });

        return results;
    }

    sortResultsByAuthority(results){
        console.log('authority ranking');
        var resultGraph = createGraph(results);
        var rank = pagerank(resultGraph);
        console.log(rank);

        results.sort(function(a, b) {return rank[b.id] - rank[a.id]});

        return results;
    }

    sortResultsByCentrality(results){
        console.log('centrality ranking');
        var resultGraph = createGraph(results);
        var rank = centrality.degree(resultGraph);
        console.log(rank);

        results.sort(function(a, b) {return rank[b.id] - rank[a.id]});

        return results;
    }

    filterRersults(results, ...tags){
        var filteredResults = JSON.parse('[]');
        var z = 0;
        console.log('Output tags: ', tags);
        for(var d in results){
            var singleInstance = JSON.parse('{}');
            for(var j in tags){
                singleInstance[tags[j]] = results[d][tags[j]];
            }
            filteredResults[z++] = singleInstance;
        }

        return filteredResults;
    }
   
}

function createGraph(raw){
    var graph = graphBuilder();
    for(d in raw){
        graph.addNode(raw[d].id);
    }
    for(d in raw){
        var currKGLinks = raw[d].links;
        for(link in currKGLinks){
            if(graph.getNode(currKGLinks[link].target) != null){
                graph.addLink(raw[d].id, currKGLinks[link].target);
            }
        }
    }

    return graph;
}

function lcToStandard(datasets) {
    var standardized = JSON.parse('[]');
    var i = 0;
    for(var ds in datasets){
        var currentItem = JSON.parse('{}');
        currentItem['id'] = datasets[ds]['_id'];
        currentItem['title'] = datasets[ds]['title'];
        currentItem['description'] = datasets[ds]['description']['en'];
        currentItem['website'] = datasets[ds]['website'];
        currentItem['triples'] = datasets[ds]['triples'];
        currentItem['keywords'] = datasets[ds]['keywords'];
        currentItem['links'] = datasets[ds]['links'];
        currentItem['sparql'] = datasets[ds]['sparql'][0];

        standardized[i++] = currentItem;
    }

    return standardized;
}

function dhToStandard(datasets) {
    var standardized = JSON.parse('[]');
    var i = 0, j = 0;
    for(var ds in datasets){
        var currentItem = JSON.parse('{}');
        var currDsLinks = datasets[ds].extras;
        var keywords = JSON.parse('[]');
        var links = JSON.parse('[]'); var currentLink = JSON.parse('{}');
        currentItem['id'] = datasets[ds]['name'];
        currentItem['title'] = datasets[ds]['title'];
        currentItem['description'] = datasets[ds]['notes'];
        currentItem['website'] = datasets[ds]['url'];
        currentItem['triples'] = '0';
        for(var item in currDsLinks){
            if(currDsLinks[item].key === 'triples')
                currentItem['triples'] = currDsLinks[item].value;
        }
        for(var key in datasets[ds].tags){
            keywords[j++] = datasets[ds].tags[key].name;
        } j = 0;
        currentItem['keywords'] = keywords;
        for(var link in datasets[ds].extras){
            if(datasets[ds].extras[link].key.includes('links')){
                var currentLink = JSON.parse('{}');
                currentLink['target'] = datasets[ds].extras[link].key.split(':')[1];
                currentLink['value'] = datasets[ds].extras[link].value;
                links[j++] = currentLink;                
            }
        } j = 0;        
        currentItem['links'] = links;
        currentItem['sparql'] = null;
        for(var res in datasets[ds].resources){
            if(datasets[ds].resources[res]['name'] != null && datasets[ds].resources[res]['name'].includes('SPARQL')){
                var sparqlEnd = JSON.parse('{}');
                sparqlEnd['title'] = datasets[ds].resources[res].name;
                sparqlEnd['description'] = datasets[ds].resources[res].description;
                sparqlEnd['access_url'] = datasets[ds].resources[res].url;
                sparqlEnd['status'] = datasets[ds].resources[res].state;

                currentItem['sparql'] = sparqlEnd;
            }            
        }

        standardized[i++] = currentItem;
    }

    return standardized;
}

const aggregator = new KGs_Results_Aggregator();

//temporaneo per analizzare i risultati, andrà eliminato 
fs.writeFile("first_approach.json", JSON.stringify(aggregator.filterRersults(aggregator.multiTagSearch('museum', 'id', 'title', 'centrality'), 'id', 'title'), null, 2), function(err) {
    if(err) {
        return console.log(err);
    }
    console.log("The file was saved!");
}); 