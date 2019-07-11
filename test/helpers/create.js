const { promisify } = require('util')
const corestore = require('corestore')
const ram = require('random-access-memory')

const MountableHypertrie = require('../..')

module.exports.create = async function (numTries, opts) {
  const sparse = false
  const tries = []
  const stores = []

  for (let i = 0; i < numTries; i++) {
    const store = corestore(ram, { sparse })
    const trie = new MountableHypertrie(store, null, { ...opts, sparse })
    await promisify(trie.ready)()
    tries.push(trie)
    stores.push(store)
  }

  const streams = replicateAll(tries, { sparse, live: true, encrypt: false })

  return { tries, stores, streams }
}

function replicateAll (tries, opts) {
  const streams = []
  const replicated = new Set()

  for (let i = 0; i < tries.length; i++) {
    for (let j = 0; j < tries.length; j++) {
      if (i === j || replicated.has(j)) continue
      const source = tries[i]
      const dest = tries[j]

      const s1 = source.replicate({ ...opts })
      const s2 = dest.replicate({ ...opts })
      streams.push([s1, s2])

      s1.pipe(s2).pipe(s1)
    } replicated.add(i)
  }
  return streams
}
