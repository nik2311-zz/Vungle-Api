const express = require('express')
const app = express()



var elasticsearch = require('elasticsearch');
const es_host = process.env.ES_HOST

var client = new elasticsearch.Client({
  host: es_host
});

client.ping({
    // ping usually has a 3000ms timeout
    requestTimeout: 1000
  }, function (error) {
    if (error) {
      console.trace('elasticsearch cluster is down!');
    } else {
      console.log('All is well');
    }
  });


app.get('/appflyermonitorstats/timeline', function (req, res) {
    client.search({
        index: 'appflyermonitorstats',
        type: 'appflyermonitorstats',
        body: {
            query: {
                range : {
                    stat_date : {
                        gte : req.query.from ? req.query.from : "2018-07-27",
                        lte : req.query.to ? req.query.to : "2018-07-27"
                    }
                }
            }
        }
      }).then(function(data){

        var items = []
        for (const tweet of data.hits.hits) {
            items.push(tweet._source)
          }
    
          res.send(items)

      },function(error){})
       
      
  })

app.listen(3000, () => console.log('Example app listening on port 3000!'))

