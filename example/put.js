var hyperkv = require('../')
var hyperlog = require('hyperlog')
var sub = require('subleveldown')

var level = require('level')
var db = level('/tmp/kv.db')

var kv = hyperkv({
  log: hyperlog(sub(db, 'log')),
  db: sub(db, 'kv')
})

var key = process.argv[2]
var value = process.argv[3]

kv.put(key, value, function (err) {
  if (err) console.error(err)
})
