var test = require('tape')
var hyperkv = require('../')
var memdb = require('memdb')
var hyperlog = require('hyperlog')

test('1.x format to 2.x format', function (t) {
  t.plan(4)

  var kv = hyperkv({
    log: hyperlog(memdb(), { valueEncoding: 'json' }),
    db: memdb()
  })

  kv.put('foo', 'bar', function (err, node) {
    t.ifError(err)
    kv.dex.ready(function () {
      // write expected old format
      kv.xdb.put('foo', [ node.key ], function (err) {
        t.ifError(err)
        // make a fetch, which will grab the value in the old db format just
        // written
        kv.get('foo', function (err, values) {
          t.ifError(err)
          var expected = {}
          expected[node.key] = { value: 'bar' }
          t.deepEqual(values, expected)
          t.end()
        })
      })
    })
  })
})
