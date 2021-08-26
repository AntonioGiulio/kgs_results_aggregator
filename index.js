const lodcloud_querier = require('lodcloud-querier');
const datahub_querier = require('datahub-querier');
const parallel = require('run-parallel');
const graphBuilder = require('ngraph.graph');
const fs = require('fs');

//lc_querier = new lodcloud_querier();
//dh_querier = new datahub_querier();

class KGs_Results_Aggregator {
    
    constructor() {
        this.lc_querier = new lodcloud_querier();
        this.dh_querier = new datahub_querier();
    }


    
    //PARALLEL
    brutalSearch(target){

        parallel([
            (callback) => {callback(null, this.lc_querier.brutalSearch(target, arguments[1]))},
            (callback) => {callback(null, this.dh_querier.brutalSearch(target, arguments[1]))}
          ],
          function (err, results) {
            var lc_results = results[0];
            var dh_results = results[1];

            var mergedResults = JSON.parse('[]');
            var i = 0;

            var dh_graph = graphBuilder();
            for(var kg in dh_results){
                dh_graph.addNode(dh_results[kg]['name']);
            }
            
            /*
            //stampiamo tutti i nodi 
            dh_graph.forEachNode(function(node){
                console.log(node.id);
            });*/

            //inizia la fase di confronto vera e propria
            for(var kg in lc_results){
                //se nell'insieme dei risultati di dh non è presente il risultato corrente di lod cloud stampo
                if(dh_graph.getNode(lc_results[kg]['_id']) == null){
                    mergedResults[i++] = lc_results[kg];
                    console.log('ho aggiunto ai risultati: ' + lc_results[kg]['_id']);
                }
            }

            //aggiungo anche tutti i risultati di dh hub;
            for(var kg in dh_results){
                mergedResults[i++] = dh_results[kg];
            }            
            fs.writeFile("first_approach.json", JSON.stringify(mergedResults, null, 2), function(err) {
              if(err) {
                  return console.log(err);
              }
              console.log("The file was saved!");
            }); 
            return mergedResults;
          })
    }

   
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
        currentItem['sparql'] = datasets[ds]['sparql'];

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

//const aggregator = new KGs_Results_Aggregator();
//var t0 = new Date().getTime();
//aggregator.brutalSearch('.*');

