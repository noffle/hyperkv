var test = require('tape')
var hyperkv = require('../')
var memdb = require('memdb')
var hyperlog = require('hyperlog')
var temp = require('temp')
var level = require('level')
var sub = require('subleveldown')

// 1. create kv instance, manually edit xdb, create new kv instance + ensure works
// 2. two subsequent uses on same version works okay

test('1.x format to 2.x format', function (t) {
  t.plan(8)

  var db
  var levelPath

  temp.track()
  temp.mkdir('hyperkv-test-bw-compat', function (err, dirPath) {
    t.ifError(err)
    levelPath = dirPath
    db = level(dirPath)
    create1xx()
  })

  function create1xx () {
    var kv = hyperkv({
      log: hyperlog(sub(db, 'log'), { valueEncoding: 'json' }),
      db: sub(db, 'kv')
    })

    kv.put('foo', 'bar', function (err, node) {
      t.ifError(err)
      kv._ready(function () {
        // write expected old format
        kv.xdb.put('foo', [ node.key ], function (err) {
          t.ifError(err)
          // fudge version value
          kv.xdbMeta.put('version', 1, function (err) {
            t.ifError(err)
            // shutdown the db and walk away
            kv.xdb.close(function (err) {
              t.ifError(err)
              kv.idb.close(function (err) {
                t.ifError(err)
                create2xx(node.key)
              })
            })
          })
        })
      })
    })
  }

  function create2xx (key) {
    // new kv
    var db = level(levelPath)
    var kv = hyperkv({
      log: hyperlog(sub(db, 'log'), { valueEncoding: 'json' }),
      db: sub(db, 'kv')
    })

    // make a fetch, which will grab the value in the old db format just
    // written
    kv._ready(function () {
      kv.get('foo', function (err, values) {
        t.ifError(err)
        var expected = {}
        expected[key] = { value: 'bar' }
        t.deepEqual(values, expected)
        t.end()
      })
    })
  }
})

test('2.x format used twice in a row', function (t) {
  t.plan(6)

  var db
  var levelPath

  temp.track()
  temp.mkdir('hyperkv-test-bw-compat', function (err, dirPath) {
    t.ifError(err)
    levelPath = dirPath
    db = level(dirPath)
    createFirst()
  })

  function createFirst () {
    var kv = hyperkv({
      log: hyperlog(sub(db, 'log'), { valueEncoding: 'json' }),
      db: sub(db, 'kv')
    })

    kv.put('foo', 'bar', function (err, node) {
      t.ifError(err)
      kv._ready(function () {
        // shutdown the db and walk away
        kv.xdb.close(function (err) {
          t.ifError(err)
          kv.idb.close(function (err) {
            t.ifError(err)
            createSecond(node.key)
          })
        })
      })
    })
  }

  function createSecond (key) {
    // new kv
    var db = level(levelPath)
    var kv = hyperkv({
      log: hyperlog(sub(db, 'log'), { valueEncoding: 'json' }),
      db: sub(db, 'kv')
    })

    kv._ready(function () {
      kv.get('foo', function (err, values) {
        t.ifError(err)
        var expected = {}
        expected[key] = { value: 'bar' }
        t.deepEqual(values, expected)
        t.end()
      })
    })
  }
})
