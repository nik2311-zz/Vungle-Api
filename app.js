const express = require('express')
const app = express()



var elasticsearch = require('elasticsearch');
const es_host = process.env.ES_HOST || 'ec2-54-218-19-172.us-west-2.compute.amazonaws.com:29200'

console.log(es_host)
var client = new elasticsearch.Client({
  host: es_host
});

client.ping({
  // ping usually has a 3000ms timeout
  requestTimeout: 1000
}, function (error) {
  if (error) {
    console.trace('elasticsearch cluster is down!', error);
  } else {
    console.log('All is well');
  }
});


app.get('/vungle/ads/timeline', function (req, res) {
  console.log(req.query.metrics)
  var fields = null
  if (req.query.metrics) {
    fields = req.query.metrics.split(',')
    fields.push("time_hour")
  }
  else {
    fields = ["time_hour", "Installs", "Advertiser Spend", "Impressions", "Completes", "Third Quartile Video", "Clicks", "Second Quartile Video", "Unique Devices", "Views", "Zero Quartile Video", "First Quartile Video", "CTR", "Completion Rate", "Conversion Rate", "Spend eCPI"]
  }
  client.search({
    index: 'vungle',
    type: 'ads',
    body: {
      _source: fields,
      from: 0, size: 10000,
      sort: [{ "time_hour": { "order": "asc" } }],
      query: {
        range: {
          time_hour: {
            gte: req.query.from ? req.query.from : "2018-01-01",
            lte: req.query.to ? req.query.to : "2019-01-28",
          }
        }
      }
    }
  }).then(function (data) {
    var items = []
    var grouped = {}
    for (const item of data.hits.hits) {
      var time_hour = item._source['time_hour']
      var src = item._source
      delete src["time_hour"]
      Object.keys(src).forEach(function (f) {
        if (grouped[f]) {
          grouped[f].push([time_hour, src[f]])
        } else {
          grouped[f] = []
        }
      });
    }

    res.send(grouped)

  }, function (error) { })


})




app.get('/vungle/ads/dimensions', function (req, res) {
  var fields = null
  if (req.query.dimensions) {
    fields = req.query.dimensions.split(',')
    fields.push("time_hour")
  }
  else {
    fields = ["time_hour", "Installs", "Advertiser Spend", "Impressions", "Completes", "Third Quartile Video", "Clicks", "Second Quartile Video", "Unique Devices", "Views", "Zero Quartile Video", "First Quartile Video", "CTR", "Completion Rate", "Conversion Rate", "Spend eCPI"]
  }

  var es_query = {
    index: 'vungle',
    type: 'ads',
    body: {
      _source: fields,
      size: 0,
      query: {
        range: {
          time_hour: {
            gte: req.query.from ? req.query.from : "2018-01-01",
            lte: req.query.to ? req.query.to : "2019-01-28",
          }
        }
      },
      aggs: {
        
      }
    }
  }

  fields.forEach(function (f) {
     if(f != "time_hour"){
      var agg_name = `total_${f}`
      es_query.body.aggs[agg_name]={"sum":{"field":f}}
     }
})

    client.search(es_query).then(function (data) {
      res.send(data.aggregations)

    }, function (error) { 
      res.send(error)
    })


  



})

app.listen(3000, () => console.log('Example app listening on port 3000!'))
