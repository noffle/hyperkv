module.exports = function (db, done) {
  var ops = []
  db.createKeyStream()
    .on('data', function (key) {
      ops.push({ type: 'del', key: key })
    })
    .once('end', function () {
      db.batch(ops, done)
    })
    .once('error', done)
}
