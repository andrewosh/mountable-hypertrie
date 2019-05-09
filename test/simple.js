const test = require('tape')

const { create } = require('./helpers/create')
const { runAll } = require('./helpers/util')

const MountableHypertrie = require('..')

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

  t.end()
})

test('simple cross-trie del', async t => {
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
        return cb(null)
      }),
      cb => rootTrie.get('/b', (err, node) => {
        if (err) return cb(err)
        t.true(node)
        t.same(node.key, 'b')
        return cb(null)
      }),
      cb => subTrie.get('/b', (err, node) => {
        if (err) return cb(err)
        t.true(node)
        t.same(node.key, 'b')
        return cb(null)
      }),
      cb => rootTrie.del('/a/b', cb),
      cb => rootTrie.get('/a/b', (err, node) => {
        if (err) return cb(err)
        t.false(node)
        return cb(null)
      }),
      cb => rootTrie.del('/b', cb), 
      cb => rootTrie.get('/b', (err, node) => {
        if (err) return cb(err)
        t.false(node)
        return cb(null)
      }),
      cb => subTrie.get('/b', (err, node) => {
        if (err) return cb(err)
        t.false(node)
        return cb(null)
      })
    ])
  } catch (err) {
    t.error(err)
  }

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

  t.end()
})

test('recursive cross-trie del', async t => {
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
      cb => rootTrie.put('/a/b/e', 'walrus', cb),
      cb => rootTrie.put('/a/c', 'potato', cb),
      cb => rootTrie.put('/a/e', 'cat', cb),
      cb => rootTrie.put('/a/b/f', 'horse', cb),
      cb => rootTrie.put('/d', 'calculator', cb),
      cb => rootTrie.del('/d', cb),
      cb => rootTrie.del('/a/b/e', cb),
      cb => rootTrie.del('/a/d', cb),
      cb => rootTrie.get('/a/d', (err, node) => {
        if (err) return cb(err)
        t.false(node)
        return cb(null)
      }),
      cb => rootTrie.get('/a/b/d', (err, node) => {
        if (err) return cb(err)
        t.true(node)
        t.same(node.value, Buffer.from('cat'))
        return cb(null)
      }),
      cb => subTrie.del('/b', cb),
      cb => rootTrie.get('/a/b/d', (err, node) => {
        if (err) return cb(err)
        t.false(node)
        return cb(null)
      }),
      cb => subsubTrie.get('/d', (err, node) => {
        if (err) return cb(err)
        t.true(node)
        t.same(node.key, 'd')
        t.same(node.value, Buffer.from('cat'))
        return cb(null)
      }),
      cb => rootTrie.get('/d', (err, node) => {
        t.false(node)
        return cb(null)
      })
    ])
  } catch (err) {
    t.error(err)
  }

  t.end()
})


test('recursive get node references the correct sub-trie', async t => {
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
        t.same(node[MountableHypertrie.Symbols.TRIE].key, subTrie.key)
        return cb(null)
      }),
      cb => rootTrie.get('/a/b/d', (err, node) => {
        if (err) return cb(err)
        t.true(node)
        t.same(node[MountableHypertrie.Symbols.TRIE].key, subsubTrie.key)
        return cb(null)
        }),
      cb => rootTrie.get('/b', (err, node) => {
        if (err) return cb(err)
        t.true(node)
        t.same(node[MountableHypertrie.Symbols.TRIE].key, rootTrie.key)
        return cb(null)
      })
    ])
  } catch (err) {
    t.error(err)
  }

  t.end()
})

