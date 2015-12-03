var sub = require('subleveldown')
var indexer = require('hyperlog-index')

module.exports = KV

function KV (opts) {
  if (!(this instanceof KV)) return new KV(opts)
  var self = this
  self.log = opts.log
  self.idb = sub(opts.db, 'i')
  self.xdb = sub(opts.db, 'x', { valueEncoding: 'json' })
  self.dex = indexer(self.log, self.idb, function (row, next) {
    self.xdb.get(row.value.k, function (err, doc) {
      if (!doc) doc = {}
      row.links.forEach(function (link) { delete doc[link] })
      doc[row.key] = row.value.v
      self.xdb.put(row.value.k, doc, next)
    })
  })
}

KV.prototype.put = function (key, value, opts, cb) {
  var self = this
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  if (!opts) opts = {}
  if (!cb) cb = noop

  var doc = { k: key, v: value }
  if (opts.links) {
    self.log.add(opts.links, doc, cb)
  } else {
    self.get(key, function (err, values) {
      if (err && !notFound(err)) cb(err)
      else self.log.add(Object.keys(values || {}), doc, cb)
    })
  }
}

KV.prototype.get = function (key, cb) {
  var self = this
  self.dex.ready(function () {
    self.xdb.get(key, cb)
  })
}

function notFound (err) {
  return err && (err.notFound || /notfound/i.test(err.message))
}
function noop () {}
