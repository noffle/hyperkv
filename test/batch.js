var test = require('tape')
var hyperkv = require('../')
var memdb = require('memdb')
var hyperlog = require('hyperlog')

test('batch', function (t) {
  t.plan(5)
  var db = memdb()
  var kv = hyperkv({
    log: hyperlog(memdb(), { valueEncoding: 'json' }),
    db: memdb()
  })
  kv.batch([
    { key: 'A', value: 123 },
    { key: 'B', value: 456 }
  ], onbatch)

  function onbatch (err, nodes) {
    t.error(err)
    kv.get('A', function (err, values) {
      t.error(err)
      var expected = {}
      expected[nodes[0].key] = 123
      t.deepEqual(values, expected, 'expected values for key A')
    })
    kv.get('B', function (err, values) {
      t.error(err)
      var expected = {}
      expected[nodes[1].key] = 456
      t.deepEqual(values, expected, 'expected values for key B')
    })
  }
})
