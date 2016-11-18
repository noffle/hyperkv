var lock = require('mutexify')

module.exports = function (prepare) {
  var ready = false
  var wait = lock()

  // hold the lock until ready; this will succeed instantly
  wait(function (release) {
    // call the user prepare function; set ready when done
    prepare(function () {
      ready = true
      release()
    })
  })

  return function (cb) {
    // already ready; return instantly
    if (ready) return cb()

    // not ready yet; wait until the lock is released
    wait(function (release) {
      release()
      cb()
    })
  }
}

