var Writable = require('readable-stream').Writable
var inherits = require('inherits')

module.exports = Processor

function Processor(opts) {
  if (!(this instanceof Processor)) return new Processor(opts)
  var self = this

  this.options = opts || {}
  var db = this.db = this.options.db
  var worker = this.worker = this.options.worker
  var feed = this.feed = this.options.feed

  this.key = opts.key || 'latest'
  this.dbOptions = opts.dbOptions || {valueEncoding: 'utf8'}

  var count
  var firstChange = typeof feed.start === 'number' ? feed.start : 1
  feed.count(function (err, n) {
    if (err) self.emit('error', new Error('feed failed to initialize'))

    count = n
    start()
  })

  this.parsedLatest = null
  db.get(this.key, this.dbOptions, function(err, latest) {
    if (self.destroyed) return
    if (err && !err.notFound) return self.emit('error', err)

    if (err) latest = firstChange - 1
    else latest = parseInt(latest)

    if (isNaN(latest)) return self.emit('error', new Error('corrupted latest: ' + latest))

    self.parsedLatest = latest
    start()
  })

  function start () {
    if (!(typeof count === 'number' && typeof self.parsedLatest === 'number')) return

    self._checkLive()
    self.feedReadStream = feed.createReadStream({since: self.parsedLatest, live: true})
    self.feedReadStream.pipe(self)
    self.feedReadStream.on('error', function(err) {
      self.emit('error', err)
    })

    self.emit('processing', self.parsedLatest)
  }

  Writable.call(this, {objectMode: true, highWaterMark: 16})
}

inherits(Processor, Writable)

Processor.prototype._checkLive = function (cb) {
  var self = this
  this.feed.onready(function () {
    var live = self.parsedLatest === self.feed.tentativeChange
    if (self._live !== live) {
      self._live = live
      if (live) self.emit('live')
    }

    if (cb) cb(null, live)
  })
}

Processor.prototype.onLive = function (cb) {
  var self = this
  this._checkLive(function (err, live) {
    if (live) return cb()

    self.once('live', cb)
  })
}

Processor.prototype._write = function(obj, enc, cb) {
  var self = this
  this.worker(obj, function(err) {
    if (err) return cb(err)
    self.parsedLatest = obj.change
    self._checkLive()
    self.db.put(self.key, obj.change.toString(), self.dbOptions, function(err) {
      if (err) return cb(err)
      cb()
    })
  })
}

Processor.prototype.destroy = function(err) {
  if (this.feedReadStream) this.feedReadStream.destroy(err)
  else self.destroyed = true
}
