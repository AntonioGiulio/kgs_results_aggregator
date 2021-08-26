# Knowledge Graphs results aggregator

## Description 
It is a javascript class created to merge and standardize the results coming from [lodcloud-querier](https://www.npmjs.com/package/lodcloud-querier) and [datahub-querier](https://www.npmjs.com/package/datahub-querier).

You can perform a **brutal search** and a **search by tag ang multiple tag**.

You can also choose to **rank** the results in 4 different ways:

* **name** (default)
* **size**
* **authority**
* **centrality**


## Basic Usage
**Install** with npm:
`npm install kgs_results_aggregator`

```javascript
// First of all you have to require the package in the code
var kgs_aggregator = require('kgs_results_aggregator');

// ..then you have to initialize the aggregator
var aggregator = new kgs_aggregator();
```

Now you are ready to exploit all the functions:

```javascript
BRUTAL SEARCH

var results = aggregator.brutalSearch('keyword', 'rankingMode'); 
//rankingMode(optional) is one of['name', 'size', 'authority', 'centrality']

MULTITAG SEARCH

var results = aggregator.multiTagSearch('keyword', 'tag_1', 'tag_2', 'tag_3', ...,  rankingMode);
// you perform the query on several tags.

```

## Available methods for a kgs\_results\_aggregator instance

* **brutalSearch(target)** : For each dataset in DataHub and LodCloud, it searches within all tags for the regular expression containing the target.
* **multiTagSearch(target, ...tags)**: For each dataset, it searches within the specified tags the regular expression containing the target.
* **filterResults(result, ...tags)**: It's a filter to return in the resulting JSON only tags specified.
* **generalSorting(result, mode)**: It's a dispatcher method to execute the ranking algorithm specified in mode parameter.
* **sortResultsBySize(results)**: Sorts results by triples number.
* **sortResultsByName(results)**: Sorts results in alphabetic order using the name.
* **sortResultsByAuthority(results)**: Sorts results by authority using the pagerank algorithm.
* **sortResultsByCentrality(results)**: Sorts results by centrality using the centrality algorithm.