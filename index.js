/*
– – – – – – – – – – – – – – – – – – – – – – – – – – – – – 
Title :  Knowledge Graphs results aggregator
Author : Antonio Giulio
URL : https://github.com/AntonioGiulio/kgs_results_aggregator
Description : TThis module is capable of aggregating the results
             deriving from the lodcloud-querier and datahub-querier npm modules,
             standardizes the JSON format and applies different types of ranking.
Created : August 26 2021
version : 0.1.0
– – – – – – – – – – – – – – – – – – – – – – – – – – – – – 
*/

// npm module able to retrieve knowledge graphs from Lod-Cloud
const lodcloud_querier = require('lodcloud-querier');
// npm module able to retrieve knowledge graphs from DataHub
const datahub_querier = require('datahub-querier');
// npm module required to create graphs
const graphBuilder = require('ngraph.graph');
// npm module required to compute pagerank on a graph
const pagerank = require('ngraph.pagerank');
// npm module required to compute centrality on a graph
const centrality = require('ngraph.centrality');

//const fs = require('fs');

class KGs_Results_Aggregator {
    
    // initialize lc & dh quereirs
    constructor() {
        this.lc_querier = new lodcloud_querier();
        this.dh_querier = new datahub_querier();
    }
    
    /*
    * Summary: For each knowledge graph retrieved from the brutalSearch on th other libraries, it 
                merges and standardize results;
    * Parameters: target (string) keyword to search, rankingMode (string) enable one of ranking modes.
    * Return: JSONArray containing the ordered, standardized & merged results.
    */
    brutalSearch(target){

        // all results are standardized
        var lc_results = lcToStandard(this.lc_querier.brutalSearch(target));
        var dh_results = dhToStandard(this.dh_querier.brutalSearch(target));

        var mergedResults = JSON.parse('[]');
        var i = 0;
        
        // we create a graph with the ids of the results from DataHub
        var dh_graph = graphBuilder();
        for(var kg in dh_results){
            dh_graph.addNode(dh_results[kg]['id']);
        }

        // matching phase begins
        for(var kg in lc_results){
            if(dh_graph.getNode(lc_results[kg]['id']) == null){
                mergedResults[i++] = lc_results[kg];
            }
        }

        // later we also add all the results from datahub
        for(var kg in dh_results){
            mergedResults[i++] = dh_results[kg];
        }      
        
        /*
        fs.writeFile("first_approach.json", JSON.stringify(mergedResults, null, 2), function(err) {
            if(err) {
                return console.log(err);
            }
            console.log("The file was saved!");
        }); */
        
        // we sort the results before returning
        return this.generalSorting(mergedResults, arguments[1]);
    }

    /*
    * Summary: It uses the brutalSearch method and then acts as a filter
                 and searches for the keyword only within the specified tags.
    * Parameters: target (string) keyword to search, tags (string[]) tags to inspect, rankingMode (string) enable one of ranking modes.
    * Return: JSONArray containing the ordered, standardized & merged results.
    */
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


    /*
    * Summary: It's a dispatcher method to execute the ranking algorithm specified in mode parameter.
    * Parameters: results (JSONArray) generated in a previous request, mode (string) ranking mode.
    * Return: JSONArray containing the ordered results.
    */
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

    /*
    * Summary: Sorts results by triples number.
    * Parameters: results (JSONArray) generated in a previous request.
    * Return: JSONArray containing the ordered results.
    */
    sortResultsBySize(results){
        console.log('size ranking');
        results.sort(function(a, b){ return b.triples - a.triples});

        return results;
    }

    /*
    * Summary: Sorts results in alphabetic order using the name(id).
    * Parameters: results (JSONArray) generated in a previous request.
    * Return: JSONArray containing the ordered results.
    */
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

    /*
    * Summary: Sorts results by authority using the pagerank algorithm
    * Parameters: results (JSONArray) generated in a previous request.
    * Return: JSONArray containing the ordered results.
    */
    sortResultsByAuthority(results){
        console.log('authority ranking');
        var resultGraph = createGraph(results);
        var rank = pagerank(resultGraph);
        console.log(rank);

        results.sort(function(a, b) {return rank[b.id] - rank[a.id]});

        return results;
    }

    /*
    * Summary: Sorts results by centrality using the centrality algorithm
    * Parameters: results (JSONArray) generated in a previous request.
    * Return: JSONArray containing the ordered results.
    */
    sortResultsByCentrality(results){
        console.log('centrality ranking');
        var resultGraph = createGraph(results);
        var rank = centrality.degree(resultGraph);
        console.log(rank);

        results.sort(function(a, b) {return rank[b.id] - rank[a.id]});

        return results;
    }

    /*
    * Summary: It's a filter to return in the resulting JSON only tags specified.
    * Parameters: results (JSONArray) generated in a previous request, tags (string[]) tag to filter in.
    * Return: JSONArray containing the filtered results.
    */
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

/*
* Summary: It's an external function that create a graph from results.
* Parameters: raw (JSONArray) generated in a previous request.
* Return: graph.
*/
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

module.exports = KGs_Results_Aggregator;