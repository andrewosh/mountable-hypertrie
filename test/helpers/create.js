const { promisify } = require('util')
const corestore = require('random-access-corestore')
const ram = require('random-access-memory')

const MountableHypertrie = require('../..')

module.exports.create = async function (numTries, opts) {
  const store = corestore(path => ram(path))
  const tries = []
  for (let i = 0; i < numTries; i++) {
    const core = store.get({ name: `trie-${i}`, main: tries.length === 0 })
    const trie = new MountableHypertrie(store, null, { ...opts, feed: core })
    await promisify(trie.ready)()
    tries.push(trie)
  }
  return { tries, store }
}
