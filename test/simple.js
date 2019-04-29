const test = require('tape')
const { create, cleanup } = require('./helpers/create')
const { runAll } = require('./helpers/util')

test('simple cross-trie put/get', async t => {
  const { tries, cores, store } = await create(2)
  const [rootTrie, subTrie] = tries

  try {
    await runAll([
      cb => rootTrie.mount('/a', subTrie.key, cb),
      cb => rootTrie.put('/b', 'hello', cb),
      cb => rootTrie.put('/a/b', 'goodbye', cb),
      cb => rootTrie.get('/a/b', (err, node) => {
        if (err) return cb(err)
        t.true(node)
        t.same(node.key, 'a/b')
        t.same(node.value, Buffer.from('goodbye'))
        return cb(null)
      }),
      cb => rootTrie.get('/b', (err, node) => {
        if (err) return cb(err)
        t.true(node)
        t.same(node.key, 'b')
        t.same(node.value, Buffer.from('hello'))
        return cb(null)
        }),
      cb => subTrie.get('/b', (err, node) => {
        if (err) return cb(err)
        t.true(node)
        t.same(node.key, 'b')
        t.same(node.value, Buffer.from('goodbye'))
        return cb(null)
      })
    ])
  } catch (err) {
    t.error(err)
  }

  await cleanup(cores, store)
  t.end()
})

test('recursive cross-trie put/get', async t => {
  const { tries, cores, store } = await create(3)
  const [rootTrie, subTrie, subsubTrie] = tries

  try {
    await runAll([
      cb => rootTrie.mount('/a', subTrie.key, cb),
      cb => subTrie.mount('/b', subsubTrie.key, cb),
      cb => rootTrie.put('/b', 'hello', cb),
      cb => subTrie.put('/c', 'dog', cb),
      cb => rootTrie.put('/a/d', 'goodbye', cb),
      cb => rootTrie.put('/a/b/d', 'cat', cb),
      cb => rootTrie.get('/a/d', (err, node) => {
        if (err) return cb(err)
        t.true(node)
        t.same(node.key, 'a/d')
        t.same(node.value, Buffer.from('goodbye'))
        return cb(null)
      }),
      cb => rootTrie.get('/a/b/d', (err, node) => {
        if (err) return cb(err)
        t.true(node)
        t.same(node.key, 'a/b/d')
        t.same(node.value, Buffer.from('cat'))
        return cb(null)
        }),
      cb => subsubTrie.get('/d', (err, node) => {
        if (err) return cb(err)
        t.true(node)
        t.same(node.key, 'd')
        t.same(node.value, Buffer.from('cat'))
        return cb(null)
      })
    ])
  } catch (err) {
    t.error(err)
  }

  await cleanup(cores, store)
  t.end()
})
