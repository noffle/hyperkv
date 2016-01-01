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
    if (!row.value) return next()
    if (row.value.k !== undefined) {
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
    } else if (row.value.d !== undefined) {
      self.xdb.put(row.value.d, [], function (err) {
        if (!err) self.emit('remove', row.value.d, row)
        next(err)
      })
    }
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
  self._put(key, { k: key, v: value }, opts, function (err, node) {
    cb(err, node)
    if (!err) self.emit('put', key, value, node)
  })
}

KV.prototype.del = function (key, opts, cb) {
  var self = this
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  if (!opts) opts = {}
  if (!cb) cb = noop
  self._put(key, { d: key }, opts, function (err, node) {
    cb(err, node)
    if (!err) self.emit('del', key, node)
  })
}

KV.prototype.get = function (key, cb) {
  var self = this
  cb = once(cb || noop)
  self.dex.ready(function () {
    self.xdb.get(key, function (err, links) {
      var values = {}
      if (!links) links = []
      var pending = links.length + 1
      links.forEach(function (link) {
        self.log.get(link, function (err, doc) {
          if (err) return cb(err)
          values[link] = doc.value.v
          if (--pending === 0) cb(null, values)
        })
      })
      if (--pending === 0) cb(null, values)
    })
  })
}

KV.prototype.createReadStream = function (opts) {
  var self = this
  if (!opts) opts = {}
  var xopts = {
    gt: opts.gt,
    gte: opts.gte,
    lt: opts.lt,
    lte: opts.lte
  }
  var stream = through.obj(write)
  self.dex.ready(function () {
    self.xdb.createReadStream(xopts).pipe(stream)
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
        stream.push(nrow)
        next()
      })
    } else {
      stream.push(nrow)
      next()
    }
  }
}

KV.prototype._put = function (key, doc, opts, cb) {
  var self = this
  if (opts.links) {
    self.log.add(opts.links, doc, cb)
  } else {
    self.dex.ready(function () {
      self.xdb.get(key, function (err, links) {
        if (err && !notFound(err)) return cb(err)
        self.log.add(links || [], doc, function (err, node) {
          cb(err, node)
        })
      })
    })
  }
}

function notFound (err) {
  return err && (err.notFound || /notfound/i.test(err.message))
}
function noop () {}
