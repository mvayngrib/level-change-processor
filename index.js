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
  feed.count(function (err, n) {
    if (err) self.emit('error', new Error('feed failed to initialize'))

    count = n
    start()
  })

  this.parsedLatest = null
  db.get(this.key, this.dbOptions, function(err, latest) {
    if (self.destroyed) return
    if (err && !err.notFound) return self.emit('error', err)

    if (err) latest = 0
    else latest = parseInt(latest)

    if (isNaN(latest)) return self.emit('error', new Error('corrupted latest: ' + latest))

    self.parsedLatest = latest
    start()
  })

  Object.defineProperty(this, 'live', {
    get: function () {
      return self.parsedLatest === self.feed.change + (self.feed.queued || 0)
    }
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

Processor.prototype._checkLive = function () {
  if (this.live) this.emit('live')
}

Processor.prototype.onLive = function (cb) {
  if (this.live) return cb()

  this.once('live', function () {
    cb()
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
