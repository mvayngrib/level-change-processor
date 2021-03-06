var _test = require('tape')
var memdb = require('memdb')
var _changesFeed = require('changes-feed')
var changeProcessor = require('./')

var initVariants = [null, { start: 0 }]
initVariants.forEach(runTests)

function runTests (feedOpts) {
  var changesFeed = function (db) {
    return _changesFeed(db, feedOpts)
  }

  var test = function (name, fn) {
    name = name + ' (' + (feedOpts ? 'custom' : 'default') + ')'
    return _test(name, fn)
  }

  test('tails the feed', function(t) {
    t.plan(3)

    var feedDb = memdb()
    var stateDb = memdb()

    var feed = changesFeed(feedDb)

    var processor = changeProcessor({
      db: stateDb,
      feed: feed,
      worker: worker
    })

    function worker(change, cb) {
      t.ok(change, 'got change')
      t.ok(change.value, 'change.value')
      t.ok(typeof change.change === 'number', 'change.change')
      cb()
    }

    feed.append('hello')
  })

  test('restarts from where it left off', function(t) {
    t.plan(7)

    var feedDb = memdb()
    var stateDb = memdb()

    var feed = changesFeed(feedDb)

    var processor = changeProcessor({
      db: stateDb,
      feed: feed,
      worker: worker
    })

    var counter = 0
    var checkpoint = feed.start + 2

    function worker(change, cb) {
      counter++
      t.ok(change, 'got change')
      t.ok(change.change < checkpoint, 'change is < ' + checkpoint)

      if (counter === 2) {
        processor.destroy()
        setTimeout(function() {
          var resumedProcessor = changeProcessor({
            db: stateDb,
            feed: feed,
            worker: resumedWorker
          })

          function resumedWorker(change, cb) {
            t.ok(change, 'got change after resume')
            t.equals(change.value.toString(), 'hello3', 'got hello3')
            t.equals(change.change, checkpoint, 'got change ' + checkpoint)
            cb()
          }
        }, 500)
      }

      cb()
    }

    feed.append('hello1')
    feed.append('hello2')
    feed.append('hello3')
  })

  test('handling errors', function(t) {
    var feedDb = memdb()
    var stateDb = memdb()

    var feed = changesFeed(feedDb)

    var processor = changeProcessor({
      db: stateDb,
      feed: feed,
      worker: worker
    })

    function worker(change, cb) {}

    processor.on('error', function(err) {
      t.ok(err, 'got err')
      t.equals(err.message, 'oh god')
      t.end()
    })

    processor.on('processing', function(latest) {
      processor.feedReadStream.destroy(new Error('oh god'))
    })
  })

  test('live', function (t) {
    t.plan(1)

    var feedDb = memdb()
    var stateDb = memdb()

    var feed = changesFeed(feedDb)

    var processed = 0
    var processor = changeProcessor({
      db: stateDb,
      feed: feed,
      worker: worker
    })

    function worker(change, cb) {
      processed++
      if (processed === 1) feed.append('world')
      cb()
    }

    feed.append('hello')
    processor.on('live', function () {
      t.equal(processed, 2)
    })
  })
}
