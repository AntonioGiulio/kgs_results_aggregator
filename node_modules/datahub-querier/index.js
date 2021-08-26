/*
– – – – – – – – – – – – – – – – – – – – – – – – – – – – – 
Title :  DataHub Querier
Author : Antonio Giulio
URL : https://github.com/AntonioGiulio/datahub_search_engine
Description : This module is able to perform various types of queries on the JSON file 
            created using the CKAN (API) linked to the site.
            It turns out to be useful for those looking for a Knowledge Graph to use 
            or for those who want to create analytics on datahub.
Created : August 23 2021
version : 0.1.0
– – – – – – – – – – – – – – – – – – – – – – – – – – – – – 
*/

// npm module required for http request/response
const http_tool = require ('xmlhttprequest').XMLHttpRequest;

const fs = require('fs');

// npm module required to create graphs
const graphBuilder = require('ngraph.graph');
// npm module required to compute pagerank on a graph
const pagerank = require('ngraph.pagerank');
// npm module required to compute centrality on a graph
const centrality = require('ngraph.centrality');

// Upload of the file containing all the KGs if present
var datasets;



class DH_Querier {

    // if datahub.json is missing this function downloads it.
    constructor() {
        try {
            datasets = require("./datahub.json");
        } catch (error) {
            console.log('Datahub.json is missing. Download....');
            this.updateDatasets();         
        }        
    }


    /*
    * Summary: For each knowledge graph, it searches within all tags 
                for the regular expression containing the target.
    * Parameters: target (string) keyword to search, rankingMode (string) enable one of ranking modes.
    * Return: JSONArray containing the ordered results.
    */
    brutalSearch(target){
        console.log('brutalSearch');
        var results = JSON.parse('[]');
        var i = 0;
        var field;
        const pattern =new RegExp(target, 'i');
        console.log(pattern);
        for(let d in datasets){
            field = JSON.stringify(datasets[d]);
            if(pattern.test(field))
                results[i++] = datasets[d];
        }

        return this.generalSorting(results, arguments[1]);
    }

    /*
    * Summary: For each knowledge graph, it searches within the specified tag 
                for the regular expression containing the target.
    * Parameters: target (string) keyword to search, tag (string) tag to inspect, rankingMode (string) enable one of ranking modes.
    * Return: JSONArray containing the ordered results.
    */
    tagSearch(target, tag){
        console.log('tagSearch')
        var results = JSON.parse('[]');
        var i = 0;
        var field;
        const pattern = new RegExp(target, 'i');
        console.log(pattern);
        for(let d in datasets){
            field = JSON.stringify(datasets[d][tag]);
            if(pattern.test(field))
                results[i++] = datasets[d];
        }

        return this.generalSorting(results, arguments[2]);
    }

    /*
    * Summary: For each knowledge graph, it searches within the specified tags 
                for the regular expression containing the target.
    * Parameters: target (string) keyword to search, tags (string[]) tags to inspect, rankingMode (string) enable one of ranking modes.
    * Return: JSONArray containing the ordered results.
    */
    multiTagSearch(target, ...tags){
        console.log('multiTagSearch');
        var results = JSON.parse('[]');
        var i = 0, j;
        var field;
        const pattern = new RegExp(target, 'i');
        console.log(pattern);
        for(let d in datasets){
            field = '';
            for(j in tags){
                field += JSON.stringify(datasets[d][tags[j]]);
            }
            if(pattern.test(field)){
                results[i++] = datasets[d];
            }
        }

        return this.generalSorting(results, arguments[arguments.length-1]);
    }

    /*
    * Summary: It's a filter to return in the resulting JSON only tags specified.
    * Parameters: results (JSONArray) generated in a previous request, tags (string[]) tag to filter in.
    * Return: JSONArray containing the filtered results.
    */
    filterResults(results, ...tags){
        var filteredResults = JSON.parse('[]');
        var j, z = 0;
        console.log('Output tags: ', tags);
        for(let d in results){
            var singleInstance = JSON.parse('{}');
            for(j in tags){
                singleInstance[tags[j]] = results[d][tags[j]];
            }
            filteredResults[z++] = singleInstance;
        }

        return filteredResults;
    }

    /*
    * Summary: It's a dispatcher method to execute the ranking algorithm specified in mode parameter.
    * Parameters: results (JSONArray) generated in a previous request, mode (string) ranking mode.
    * Return: JSONArray containing the ordered results.
    */
    generalSorting(results, mode){
        switch(mode){
            case 'size':
                return this.sortResultBySize(results);
                break;

            case 'name':
                return this.sortResultByName(results);
                break;

            case 'authority':
                return this.sortResultByAuthority(results);
                break;

            case 'centrality':
                return this.sortResultByCentrality(results);
                break;

            default:
                return this.sortResultByName(results);
        }
    }

    /*
    * Summary: Sorts results by triples number.
    * Parameters: results (JSONArray) generated in a previous request.
    * Return: JSONArray containing the ordered results.
    */
    sortResultBySize(results){
        console.log('Size ranking');       

        results.sort(function(a,b){ 
            var triples_a = 0;
            var triples_b = 0;
            for (var data in a.extras){
                if(a.extras[data].key === 'triples')
                    triples_a = a.extras[data].value; 
            }
            for (var data in b.extras){
                if(b.extras[data].key === 'triples')
                    triples_b = b.extras[data].value;
            }
            return triples_b - triples_a});

        return results;
    }

    /*
    * Summary: Sorts results in alphabetic order using the name(= identifier on lodcloud).
    * Parameters: results (JSONArray) generated in a previous request.
    * Return: JSONArray containing the ordered results.
    */
    sortResultByName(results){
        console.log('Alphabetic ranking');

        results.sort(function(a, b){
            var x = a.name.toLowerCase();
            var y = b.name.toLowerCase();
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
    sortResultByAuthority(results){
        console.log('Authority ranking');
        var resultGraph = createGraph(results);
        var rank = pagerank(resultGraph);
        //console.log(rank);

        results.sort(function(a, b) { return rank[b.name] - rank[a.name]});

        return results;
    }

    /*
    * Summary: Sorts results by centrality using the centrality algorithm
    * Parameters: results (JSONArray) generated in a previous request.
    * Return: JSONArray containing the ordered results.
    */
    sortResultByCentrality(results){
        console.log('Centrality ranking');
        var resultGraph = createGraph(results);
        var rank = centrality.degree(resultGraph);
        //console.log(rank);

        results.sort(function(a, b) {return rank[b.name] - rank[a.name]});

        return results;
    }

    // we can use this function to update the KGs database
    updateDatasets(){
        var request = new http_tool();
        this.datasets = JSON.parse('[]');
        var start = 0;
        for(var i = 0; i < 12; i++){
            request.open('GET',  "https://old.datahub.io/api/3/action/package_search?&rows=100000&start=" + start, false);
            request.send();

            if(request.status === 200){
                console.log("request n':" + i + " response: " + request.status);
                var currentDS = JSON.parse(request.responseText)['result']['results'];
                var j = 0;
                for(let data in currentDS){
                    this.datasets[(j++)+start] = currentDS[data];
                }
            }
            start += 1000;
        }
        fs.writeFile('datahub.json', JSON.stringify(this.datasets), function(err) {
            if (err) return console.log(err);
            console.log('File updated');
            datasets = require("./datahub.json");
        });
    }
}

/*
* Summary: It's an external function that create a graph from results.
* Parameters: raw (JSONArray) generated in a previous request.
* Return: graph.
*/
function createGraph(raw){
    // create an empty graph
    var graph = graphBuilder();
    // let's fill it with the nodes that represent the identifiers of the datasets resulting from the search on the DataHub
    for(d in raw){
        graph.addNode(raw[d].name);
    }

    // we look for direct links between the nodes created
    for(d in raw){
        var currKGLinks = raw[d].extras;
        for(link in currKGLinks){
            if(currKGLinks[link].key.includes('links')){
                var currLink = currKGLinks[link].key.split(':')[1];
                if(graph.getNode(currLink) != null){
                    graph.addLink(raw[d].name, currLink);
                }
            }
        }
    }

    return graph;
}

module.exports = DH_Querier;