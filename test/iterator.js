const test = require('tape')
const { create, cleanup } = require('./helpers/create')
const { runAll } = require('./helpers/util')

test('simple single-trie iterator', async t => {
  const { tries, cores, store } = await create(1)
  const [rootTrie] = tries

  const vals = ['a', 'b', 'c']
  const expected = toMap(vals)

  try {
    await put(rootTrie, vals)
    await runAll([
      cb => {
        all(rootTrie.iterator(), (err, map) => {
          t.error(err, 'no error')
          t.same(map, expected, 'iterated all values')
          return cb(null)
        })
      }
    ])
  } catch (err) {
    t.error(err)
  }

  await cleanup(cores, store)
  t.end()
})

test('one-level nested iterator', async t => {
  const { tries, cores, store } = await create(3)
  const [rootTrie, aTrie, dTrie] = tries

  const vals = ['b', 'c', 'a/a', 'a/b', 'd/e', 'd/f']
  const expected = toMap(vals)

  try {
    await put(rootTrie, ['b', 'c'])
    await put(aTrie, ['a', 'b'], 'a/')
    await put(dTrie, ['e', 'f'], 'd/')
    await runAll([
      cb => rootTrie.mount('a/', aTrie.key, cb),
      cb => rootTrie.mount('d/', dTrie.key, cb),
      cb => {
        all(rootTrie.iterator(), (err, map) => {
          t.error(err, 'no error')
          t.same(map, expected, 'iterated all values')
          return cb(null)
        })
      }
    ])
  } catch (err) {
    t.error(err)
  }

  await cleanup(cores, store)
  t.end()
})

test('multi-level nested iterator', async t => {
  const { tries, cores, store } = await create(3)
  const [rootTrie, aTrie, abTrie] = tries

  const vals = ['b', 'c', 'a/a', 'a/b/c', 'a/b/d', 'a/c', 'e']
  const expected = toMap(vals)

  try {
    await put(rootTrie, ['b', 'c', 'e'])
    await put(aTrie, ['a', 'c'], 'a/')
    await put(abTrie, ['c', 'd'], 'a/b/')
    await runAll([
      cb => rootTrie.mount('a/', aTrie.key, cb),
      cb => aTrie.mount('b/', abTrie.key, cb),
      cb => {
        all(rootTrie.iterator(), (err, map) => {
          t.error(err, 'no error')
          t.same(map, expected, 'iterated all values')
          return cb(null)
        })
      }
    ])
  } catch (err) {
    t.error(err)
  }

  await cleanup(cores, store)
  t.end()

})
test('list iterator')

// Duplicated from hypertrie.
function toMap (list) {
  const map = {}
  for (var i = 0; i < list.length; i++) {
    map[list[i]] = list[i]
  }
  return map
}

function all (ite, cb) {
  const vals = {}

  ite.next(function loop (err, node) {
    if (err) return cb(err)
    if (!node) return cb(null, vals)
    const key = Array.isArray(node) ? node[0].key : node.key
    if (vals[key]) return cb(new Error('duplicate node for ' + key))
    vals[key] = Array.isArray(node) ? node.map(n => n.value.toString('utf8')).sort() : node.value.toString('utf8')
    ite.next(loop)
  })
}

function put (trie, vals, prefix = '') {
  return runAll(vals.map(v => (cb) => trie.put(v, prefix + v, cb)))
}
