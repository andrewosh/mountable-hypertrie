const { promisify } = require('util')
const corestore = require('corestore')
const rimraf = require('rimraf')

const mountableHypertrie = require('../..')

const STORAGE = 'test-storage'

module.exports.create = async function (numTries) {
  const store = corestore(STORAGE, { network: { disable: true } })
  await store.ready()
  const cores = []
  const tries = []
  for (let i = 0; i < numTries; i++) {
    const factory = (key, opts) => {
      const core = store.get(key, opts)
      cores.push(core)
      return core
    }
    const trie = mountableHypertrie(factory, null)
    await promisify(trie.ready)()
    tries.push(trie)
  }
  return { tries, cores, store }
}

module.exports.cleanup = async function (cores, store) {
  await store.close()
  return new Promise((resolve, reject) => {
    rimraf(STORAGE, err => {
      if (err) return reject(err)
      return resolve()
    })
  })
}
