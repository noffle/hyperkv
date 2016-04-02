var sub = require('subleveldown')
var inherits = require('inherits')
var indexer = require('hyperlog-index')
var once = require('once')
var has = require('has')
var EventEmitter = require('events').EventEmitter
var readonly = require('read-only-stream')
var through = require('through2')
var Readable = require('readable-stream').Readable
var xtend = require('xtend')
var defined = require('defined')

module.exports = KV
inherits(KV, EventEmitter)

function KV (opts) {
  if (!(this instanceof KV)) return new KV(opts)
  var self = this
  EventEmitter.call(self)
  self.log = opts.log
  self.idb = sub(opts.db, 'i')
  self.xdb = sub(opts.db, 'x', { valueEncoding: 'json' })
  self.dex = indexer({
    log: self.log,
    db: self.idb,
    map: mapfn
  })
  function mapfn (row, next) {
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
      self.xdb.get(row.value.d, function (err, keys) {
        var doc = {}
        ;(keys || []).forEach(function (key) { doc[key] = true })
        row.links.forEach(function (link) { delete doc[link] })
        var keys = Object.keys(doc)
        if (keys.length === 0) {
          self.xdb.del(row.value.d, onput)
        } else self.xdb.put(row.value.d, keys, onput)

        function onput (err) {
          if (!err) self.emit('remove', row.value.d, row)
          next(err)
        }
      })
    } else next()
  }
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

KV.prototype.createHistoryStream = function (key, opts) {
  var self = this
  if (!opts) opts = {}
  var stream = new Readable({ objectMode: true })
  var queue = null, reading = false
  stream._read = function () {
    if (!queue) {
      reading = true
      return
    }
    if (queue.length === 0) return stream.push(null)
    var q = queue.shift()
    if (has(seen, q)) return stream._read()
    seen[q] = true
    self.log.get(q, onget)
  }

  var seen = {}
  self.dex.ready(function () {
    self.xdb.get(key, function (err, heads) {
      queue = heads
      if (reading) stream._read()
    })
  })
  return stream

  function onget (err, doc) {
    if (err) return stream.emit('error', err)
    var rdoc = {
      key: doc.value ? doc.value.k : null,
      link: doc.key,
      value: doc.value ? doc.value.v : null,
      links: doc.links
    }
    if (doc.identity) rdoc.identity = doc.identity
    if (doc.signature) rdoc.signature = doc.signature
    ;(doc.links || []).forEach(function (link) {
      if (!has(seen, link)) queue.push(link)
    })
    stream.push(rdoc)
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

KV.prototype.batch = function (rows, opts, cb) {
  var self = this
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  cb = once(cb || noop)
  if (!opts) opts = {}

  var batch = []
  for (var i = 0; i < rows.length; i++) {
    var row = rows[i]
    if (row.type === 'put') {
      batch.push({
        value: { k: row.key, v: row.value },
        links: row.links
      })
    } else if (row.type === 'del') {
      batch.push({
        value: { d: row.key },
        links: row.links
      })
    } else if (row.type) {
      return ntick(cb, 'batch type not recognized')
    } else if (!row.type) return ntick(cb, 'batch type not provided')
  }

  if (batch.every(hasLinks)) return commit()

  self.dex.ready(function () {
    var pending = batch.length + 1
    batch.forEach(function (row) {
      var key = defined(row.value.k, row.value.d)
      self.xdb.get(key, function (err, links) {
        if (err && !notFound(err)) return cb(err)
        row.links = links
        if (--pending === 0) commit()
      })
    })
    if (--pending === 0) commit()
  })

  function commit () {
    self.log.batch(batch, cb)
  }
  function hasLinks (row) { return row.links !== undefined }
}

function notFound (err) {
  return err && (err.notFound || /notfound/i.test(err.message))
}
function noop () {}
function ntick (cb, msg) {
  var err = new Error(msg)
  process.nextTick(function () { cb(err) })
}
