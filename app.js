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


app.get('/vungle/ads/timeline', function (req, res) {
  console.log(req.query.metrics)
  var fields = null
  if(req.query.metrics){
    fields=req.query.metrics.split(',')
  }
  else{
    fields=["time_hour","Installs","Advertiser Spend","Impressions","Completes","Third Quartile Video","Clicks","Second Quartile Video","Unique Devices","Views","Zero Quartile Video","First Quartile Video","CTR","Completion Rate","Conversion Rate","Spend eCPI"]
  }
    client.search({
        index: 'vungle',
        type: 'ads',
        body: {
              _source: fields,
              query: {
                range: {
                  time_hour: {
                    gte : req.query.from ? req.query.from : "2018-01-01",
                    lte : req.query.to ? req.query.to :"2019-01-28",
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
