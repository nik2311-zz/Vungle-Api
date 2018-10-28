const express = require('express')
const app = express()
const cors = require('cors')


app.use(cors())



var elasticsearch = require('elasticsearch');
const es_host = 'ec2-54-218-19-172.us-west-2.compute.amazonaws.com:29200'

console.log(es_host)
var client = new elasticsearch.Client({
  host: es_host
});

client.ping({
  // ping usually has a 3000ms timeout
  requestTimeout: 5000
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
          time_hour=new Date(time_hour).valueOf()
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
  
  var agg_func = req.query.agg || "sum"
  fields.forEach(function (f) {
     if(f != "time_hour"){
      var agg_name = f
      //es_query.body.aggs[agg_name]={"avg":{"field":f}}
      es_query.body.aggs[agg_name]={}
      es_query.body.aggs[agg_name][agg_func] = {"field":f}
     }
})

    client.search(es_query).then(function (data) {
      res.send(data.aggregations)

    }, function (error) { 
      res.send(error)
    })


  



})






app.get('/vungle/adspend/platform/', function (req, res) {
  client.search({
  "index": 'vungle_adid_dimenstion',
  "body":{
    query: {
        range: {
          time_hour: {
            gte: req.query.from ? req.query.from : "2018-01-01",
            lte: req.query.to ? req.query.to : "2019-01-28",
          }
        }
      },
    "aggregations": {
      "advertiser_spend": {
         "aggregations": {
            "ad_spend": {
               "sum": {
                  "field": "Advertiser Spend"
               }
            }
         },
         "terms": {
            "field": "Platform.keyword",
            "order": {
               "ad_spend": "desc"
            }
         }
      }
   }}
   ,
   "size": 0
}).then(function (data) {
    console.log(data)
    let buckets = data.aggregations.advertiser_spend.buckets
    let groups = {}
    buckets.forEach(function(item){
      groups[item.key]=item.ad_spend.value
    })
    res.send(groups)

  }, function (error) { })


})




app.get('/vungle/adspend/campaign/', function (req, res) {
  client.search({
  "index": 'vungle_ad_campaign',
  "body":{
    query: {
        range: {
          time_hour: {
            gte: req.query.from ? req.query.from : "2018-01-01",
            lte: req.query.to ? req.query.to : "2019-01-28",
          }
        }
      },
    "aggregations": {
      "advertiser_spend": {
         "aggregations": {
            "ad_spend": {
               "sum": {
                  "field": "Advertiser Spend"
               }
            }
         },
         "terms": {
            "field": "Campaign Name.keyword",
            "order": {
               "ad_spend": "desc"
            }
         }
      }
   }}
   ,
   "size": 0
}).then(function (data) {
    console.log(data)
    let buckets = data.aggregations.advertiser_spend.buckets
    let groups = {}
    buckets.forEach(function(item){
      groups[item.key]=item.ad_spend.value
    })
    res.send(groups)

  }, function (error) { })


})



app.get('/vungle/adspend/advertiserid/', function (req, res) {
  client.search({
  "index": 'vungle_adid_dimenstion',
  "body":{
    query: {
        range: {
          time_hour: {
            gte: req.query.from ? req.query.from : "2018-01-01",
            lte: req.query.to ? req.query.to : "2019-01-28",
          }
        }
      },
    "aggregations": {
      "advertiser_spend": {
         "aggregations": {
            "ad_spend": {
               "sum": {
                  "field": "Advertiser Spend"
               }
            }
         },
         "terms": {
            "field": "Advertiser ID.keyword",
            "order": {
               "ad_spend": "desc"
            }
         }
      }
   }}
   ,
   "size": 0
}).then(function (data) {
    console.log(data)
    let buckets = data.aggregations.advertiser_spend.buckets
    let groups = {}
    buckets.forEach(function(item){
      groups[item.key]=item.ad_spend.value
    })
    res.send(groups)

  }, function (error) { })


})

app.get('/vungle/adspend/creativename/', function (req, res) {
  client.search({
  "index": 'vungle_ad_creative',
  "body":{
    query: {
        range: {
          time_hour: {
            gte: req.query.from ? req.query.from : "2018-01-01",
            lte: req.query.to ? req.query.to : "2019-01-28",
          }
        }
      },
    "aggregations": {
      "advertiser_spend": {
         "aggregations": {
            "ad_spend": {
               "sum": {
                  "field": "Advertiser Spend"
               }
            }
         },
         "terms": {
            "field": "Creatve Name.keyword",
            "order": {
               "ad_spend": "desc"
            }
         }
      }
   }}
   ,
   "size": 0
}).then(function (data) {
    console.log(data)
    let buckets = data.aggregations.advertiser_spend.buckets
    let groups = {}
    buckets.forEach(function(item){
      groups[item.key]=item.ad_spend.value
    })
    res.send(groups)

  }, function (error) { })


})






app.get('/vungle/adspend/', function (req, res) {


  let out = {}

  client.search({
  "index": 'vungle_ad_creative',
  "body":{
    query: {
        range: {
          time_hour: {
            gte: req.query.from ? req.query.from : "2018-01-01",
            lte: req.query.to ? req.query.to : "2019-01-28",
          }
        }
      },
    "aggregations": {
      "advertiser_spend": {
         "aggregations": {
            "ad_spend": {
               "sum": {
                  "field": "Advertiser Spend"
               }
            }
         },
         "terms": {
            "field": "Creatve Name.keyword",
            "order": {
               "ad_spend": "desc"
            }
         }
      }
   }}
   ,
   "size": 0
}).then(function (data) {
    let buckets = data.aggregations.advertiser_spend.buckets
    let groups = {}
    buckets.forEach(function(item){
      groups[item.key]=item.ad_spend.value
    })
    out['creativename']=groups
    groups={}
    client.search({
  "index": 'vungle_adid_dimenstion',
  "body":{
    query: {
        range: {
          time_hour: {
            gte: req.query.from ? req.query.from : "2018-01-01",
            lte: req.query.to ? req.query.to : "2019-01-28",
          }
        }
      },
    "aggregations": {
      "advertiser_spend": {
         "aggregations": {
            "ad_spend": {
               "sum": {
                  "field": "Advertiser Spend"
               }
            }
         },
         "terms": {
            "field": "Advertiser ID.keyword",
            "order": {
               "ad_spend": "desc"
            }
         }
      }
   }}
   ,
   "size": 0
}).then(function (data) {
    console.log(data)
    let buckets = data.aggregations.advertiser_spend.buckets
    let groups = {}
    buckets.forEach(function(item){
      groups[item.key]=item.ad_spend.value
    })
    out['advertiserid']=groups
    groups={}
    client.search({
  "index": 'vungle_ad_campaign',
  "body":{
    query: {
        range: {
          time_hour: {
            gte: req.query.from ? req.query.from : "2018-01-01",
            lte: req.query.to ? req.query.to : "2019-01-28",
          }
        }
      },
    "aggregations": {
      "advertiser_spend": {
         "aggregations": {
            "ad_spend": {
               "sum": {
                  "field": "Advertiser Spend"
               }
            }
         },
         "terms": {
            "field": "Campaign Name.keyword",
            "order": {
               "ad_spend": "desc"
            }
         }
      }
   }}
   ,
   "size": 0
}).then(function (data) {
    console.log(data)
    let buckets = data.aggregations.advertiser_spend.buckets
    let groups = {}
    buckets.forEach(function(item){
      groups[item.key]=item.ad_spend.value
    })
    out['campaign']=groups
    groups={}
    client.search({
  "index": 'vungle_adid_dimenstion',
  "body":{
    query: {
        range: {
          time_hour: {
            gte: req.query.from ? req.query.from : "2018-01-01",
            lte: req.query.to ? req.query.to : "2019-01-28",
          }
        }
      },
    "aggregations": {
      "advertiser_spend": {
         "aggregations": {
            "ad_spend": {
               "sum": {
                  "field": "Advertiser Spend"
               }
            }
         },
         "terms": {
            "field": "Platform.keyword",
            "order": {
               "ad_spend": "desc"
            }
         }
      }
   }}
   ,
   "size": 0
}).then(function (data) {
    console.log(data)
    let buckets = data.aggregations.advertiser_spend.buckets
    let groups = {}
    buckets.forEach(function(item){
      groups[item.key]=item.ad_spend.value
    })
    out['platform']=groups
    groups={}
    res.send(out)

  }, function (error) { })

  }, function (error) { })

  }, function (error) { })

  }, function (error) { })


})





app.get('/appsflyer/dimensions', function (req, res) {
  var fields = null
  if (req.query.dimensions) {
    fields = req.query.dimensions.split(',')
    fields.push("stat_date")
  }
  else {
    fields = ["stat_date","Sessions","Loyal Users","Loyal Users/Installs","Total Revenue","ARPU","email_unique_viewers","email_event_counters","email_sales_usd","facebook_unique_viewers","facebook_event_counters","facebook_sales_usd","phone_unique_viewers","phone_event_counters","phone_sales_usd","unique_viewers","event_counters","sales_usd"]
  }

  var es_query = {
    index: 'appflyer',
    type: 'appflyer',
    body: {
      _source: fields,
      size: 0,
      query: {
        range: {
          stat_date: {
            gte: req.query.from ? req.query.from : "2018-01-01",
            lte: req.query.to ? req.query.to : "2019-01-28",
          }
        }
      },
      aggs: {
        
      }
    }
  }
  
  var agg_func = req.query.agg || "sum"
  fields.forEach(function (f) {
     if(f != "stat_date"){
      var agg_name = f
      //es_query.body.aggs[agg_name]={"avg":{"field":f}}
      es_query.body.aggs[agg_name]={}
      es_query.body.aggs[agg_name][agg_func] = {"field":f}
     }
})

    client.search(es_query).then(function (data) {
      res.send(data.aggregations)

    }, function (error) { 
      res.send(error)
    })


  



})



app.get('/appsflyer/timeline', function (req, res) {
  console.log(req.query.metrics)
  var fields = null
  if (req.query.metrics) {
    fields = req.query.metrics.split(',')
    fields.push("stat_date")
  }
  else {
    fields = ["stat_date","Sessions","Loyal Users","Loyal Users/Installs","Total Revenue","ARPU","email_unique_viewers","email_event_counters","email_sales_usd","facebook_unique_viewers","facebook_event_counters","facebook_sales_usd","phone_unique_viewers","phone_event_counters","phone_sales_usd","unique_viewers","event_counters","sales_usd"]
  }
  client.search({
    index: 'appflyer',
    type: 'appflyer',
    body: {
      _source: fields,
      from: 0, size: 10000,
      sort: [{ "stat_date": { "order": "asc" } }],
      query: {
        range: {
          stat_date: {
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
      var time_hour = item._source['stat_date']
      var src = item._source
      delete src["stat_date"]
      Object.keys(src).forEach(function (f) {
        if (grouped[f]) {
          time_hour = new Date(time_hour).valueOf()
          grouped[f].push([time_hour, src[f]])
        } else {
          grouped[f] = []
        }
      });
    }

    res.send(grouped)

  }, function (error) { })


})


GOOGLE_FIELD_MAP = {

  'Cost / conv.':'CostConv',
  'Avg. CPC':'AvgCPC',
  'Conv. rate':'ConvRate',
  'View-through conv.':'ViewThroughConv',
  'CostConv':'Cost / conv.',
  'AvgCPC':'Avg. CPC',
  // 'ConvRate':'Conv. rate'
  // 'ViewThroughConv':'View-through conv.'

}

var add_all = function(all_data,dateepoch,fielname){ return all_data[fielname][dateepoch].reduce(function(a,b){ return a+b})}

var ctr_group_by_day = function(all_data,dateepoch){
                    return (all_data['Clicks'][dateepoch].reduce(function(a,b){ return a+b})/all_data['Impressions'][dateepoch].reduce(function(a,b){ return a+b}))*100
}

var ctr_per_conv_group_by_day = function(all_data,dateepoch){
  return (all_data['Cost'][dateepoch].reduce(function(a,b){ return a+b})/all_data['Conversions'][dateepoch].reduce(function(a,b){ return a+b}))*100
}

var all_string = function(item){ return "all"}

var currency_string = function(item){ return "USD"}

GROUP_BY = {'CTR':ctr_group_by_day,
  'Impressions':add_all,
  'CostConv':ctr_per_conv_group_by_day,
  'Cost':add_all,
  'Ad group state':all_string,
  'Campaign':all_string,
  'AvgCPC':add_all,
  'Conversions':add_all,
  'Ad group':all_string,
  'Currency':currency_string,
  'ConvRate':all_string,
  'Clicks':add_all,
  'Campaign subtype':all_string,
  'Campaign type':all_string}

app.get('/google/timeline', function (req, res) {
  console.log(req.query.metrics)
  var fields = null
  if (req.query.metrics) {
    fields = []
    req.query.metrics.split(',').forEach(function (f){
      var nf=GOOGLE_FIELD_MAP[f]|| f
      fields.push(nf)
    })
    fields.push("Day")
  }
  else {
    fields = ['Ad group', 'Ad group state', 'Campaign', 'Campaign type',
    'Campaign subtype', 'Day', 'Currency', 'Clicks', 'Impressions', 'CTR',
    'Avg. CPC', 'Cost', 'Conversions', 'Cost / conv.',
    'Conv. rate']
  }
  client.search({
    index: 'google_ad_group',
    type: 'google_ad_group',
    body: {
      _source: fields,
      from: 0, size: 10000,
      sort: [{ "Day": { "order": "asc" } }],
      query: {
        range: {
          Day: {
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
      var time_hour = item._source['Day']
      var src = item._source
      delete src["Day"]
      Object.keys(src).forEach(function (f) {
        var nf=GOOGLE_FIELD_MAP[f]|| f
        if (grouped[nf]) {
          time_hour = new Date(time_hour).valueOf()
          if(grouped[nf][time_hour]){
            grouped[nf][time_hour].push(src[f])
          }else{
            grouped[nf][time_hour]=[src[f]]
          }

          // grouped[nf].push([time_hour, src[f]])
        } else {
          grouped[nf] = {}
        }
      });
    }
    
    // console.log(grouped['CTR'][0])
    out = {}
    Object.keys(grouped).forEach(function (f){
      console.log(f)
      Object.keys(grouped[f]).forEach(function(day){
          console.log(day,grouped[f][day],GROUP_BY[f])
          var reduced_val = GROUP_BY[f](grouped,day,f)
          console.log(day,reduced_val,grouped[f][day])
          if(out[f]){
            out[f].push([day,reduced_val])
          }else{
            out[f]=[[day,reduced_val]]
          }
      })
    })
    
    var tc=[]
    out['Clicks'].forEach(function(a){
      // console.log(a[1]+b[1])
      tc.push(a[1])
    })

    var ti =[]
    out['Impressions'].forEach(function(a){
      // console.log(a[1]+b[1])
      ti.push(a[1])
    })
    tc = tc.reduce(function(a,b){return a+b})
    ti = ti.reduce(function(a,b){return a+b})
    console.log('totalclicks ',tc)
    out['total']={'Clicks':tc,'Impressions':ti,'Ctr':(tc/ti)*100}

    
    // out['total']['Ctr'] = (out['total']['Clicks'] / out['total']['Impressions'])*100 
    res.send(out)

  }, function (error) { })


})



app.get('/google/dimensions', function (req, res) {
  var fields = null
  if (req.query.dimensions) {
    fields = []
    req.query.dimensions.split(',').forEach(function (f){
      var nf=GOOGLE_FIELD_MAP[f]|| f
      fields.push(nf)
    })
    fields.push("Day")
  }
  else {
    fields = fields = ['Clicks', 'Impressions', 'CTR',
    'Avg. CPC', 'Cost', 'Conversions', 'View-through conv.', 'Cost / conv.',
    'Conv. rate']
  }

  var es_query = {
    index: 'google_ad_group',
    type: 'google_ad_group',
    body: {
      _source: fields,
      size: 0,
      query: {
        range: {
          Day: {
            gte: req.query.from ? req.query.from : "2018-01-01",
            lte: req.query.to ? req.query.to : "2019-01-28",
          }
        }
      },
      aggs: {
        
      }
    }
  }
  
  var agg_func = req.query.agg || "sum"
  fields.forEach(function (f) {
     if(f != "Day"){
      var agg_name = f
      //es_query.body.aggs[agg_name]={"avg":{"field":f}}
      es_query.body.aggs[agg_name]={}
      es_query.body.aggs[agg_name][agg_func] = {"field":f}
     }
})

    client.search(es_query).then(function (data) {
      response = {}
      Object.keys(data.aggregations).forEach(function(f){
        console.log(f)
        var nf=GOOGLE_FIELD_MAP[f]|| f
        response[nf] = data.aggregations[f]
      })
      res.send(response)

    }, function (error) { 
      res.send(error)
    })


  



})

app.listen(3000, () => console.log('Example app listening on port 3000!'))