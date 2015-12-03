var sub = require('subleveldown')
var inherits = require('inherits')
var indexer = require('hyperlog-index')
var once = require('once')
var EventEmitter = require('events').EventEmitter
var readonly = require('read-only-stream')
var through = require('through2')

module.exports = KV
inherits(KV, EventEmitter)

function KV (opts) {
  if (!(this instanceof KV)) return new KV(opts)
  var self = this
  EventEmitter.call(self)
  self.log = opts.log
  self.idb = sub(opts.db, 'i')
  self.xdb = sub(opts.db, 'x', { valueEncoding: 'json' })
  self.dex = indexer(self.log, self.idb, function (row, next) {
    self.xdb.get(row.value.k, function (err, keys) {
      var doc = {}
      ;(keys || []).forEach(function (key) { doc[key] = true })
      row.links.forEach(function (link) { delete doc[link] })
      doc[row.key] = true
      self.xdb.put(row.value.k, Object.keys(doc), function (err) {
        if (!err) self.emit('update', row.value.k, row.value.v, row)
        next(err)
      })
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
    self.dex.ready(function () {
      self.xdb.get(key, function (err, links) {
        if (err && !notFound(err)) return cb(err)
        self.log.add(links || [], doc, function (err, node) {
          cb(err, node)
          self.emit('put', key, value, node)
        })
      })
    })
  }
}

KV.prototype.get = function (key, cb) {
  var self = this
  cb = once(cb || noop)
  self.dex.ready(function () {
    self.xdb.get(key, function (err, links) {
      var values = {}
      if (!links) links = []
      var pending = links.length
      links.forEach(function (link) {
        self.log.get(link, function (err, doc) {
          if (err) return cb(err)
          values[link] = doc.value.v
          if (--pending === 0) cb(null, values)
        })
      })
    })
  })
}

KV.prototype.createReadStream = function (opts) {
  var self = this
  if (!opts) opts = {}
  var stream = through.obj(write)
  self.dex.ready(function () {
    self.xdb.createReadStream().pipe(stream)
  })
  return readonly(stream)

  function write (row, enc, next) {
    var nrow = {
      key: row.key,
      links: row.value
    }
    if (opts.values !== false) {
      self.get(row.key, function (err, values) {
        if (err) return next(err)
        nrow.values = values
        next()
      })
    } else {
      self.push(nrow)
      next()
    }
  }
}

function notFound (err) {
  return err && (err.notFound || /notfound/i.test(err.message))
}
function noop () {}
