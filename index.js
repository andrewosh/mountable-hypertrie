const p = require('path')
const hypertrie = require('hypertrie')
const thunky = require('thunky')
const nanoiterator = require('nanoiterator')
const isOptions = require('is-options')
const unixify = require('unixify')

const { Mount } = require('./lib/messages')

const Flags = {
  MOUNT: 1
}
const MOUNT_PREFIX = '/mounts'

module.exports = mountableHypertrie
function mountableHypertrie (...args) {
  return new MountableHypertrie(...args)
}

class MountableHypertrie {
  constructor (factory, key, opts) {
    this.factory = factory
    this.key = key
    this.opts = opts

    // Set in _ready.
    this._trie = null
    // TODO: Replace with a LRU cache.
    this._tries = new Map()
    this._checkouts = new Map()

    this.ready = thunky(this._ready.bind(this))
  }

  _ready (cb) {
    this._trie = hypertrie(null, { feed: this.factory(this.key, this.opts), ...this.opts })
    this._trie.ready(err => {
      if (err) return cb(err)
      this.key = this._trie.key
      return cb(null)
    })
  }

  _createHypertrie (key, opts, cb) {
    const self = this

    var versionedTrie = (opts && opts.version) ? this._checkouts.get(`${key}:${opts.version}`) : null
    if (versionedTrie) return process.nextTick(cb, null, versionedTrie)

    var trie = this._tries.get(key) || mountableHypertrie(this.factory, key, opts)
    self._tries.set(key, trie)

    if (!trie.opened) {
      trie.ready(err => {
        if (err) return cb(err)
        onready()
      })
    } else process.nextTick(onready)

    function onready () {
      if (!opts || !opts.version) return cb(null, trie)
      versionedTrie = trie.checkout(opts.version)
      this._checkouts.set(`${key}:${opts.version}`, versionedTrie)
      return cb(null, versionedTrie)
    }
  }

  _trieForMountNode (mountNode, cb) {
    if (!mountNode) return cb(new Error(`Mount metadata not found`))
    try {
      var mountInfo = Mount.decode(mountNode.value)
    } catch (err) {
      return cb(err)
    }
    this._createHypertrie(mountInfo.key, { version: mountInfo.version }, (err, trie) => {
      if (err) return cb(err)
      return cb(null, trie, mountInfo)
    })
  }

  mount (path, key, opts, cb) {
    if (typeof opts === 'function') return this.mount(path, key, null, opts)
    path = normalize(path)

    const mountRecord = Mount.encode({
      key,
      localPath: path,
      remotePath: opts && opts.remotePath && normalize(opts.remotePath),
      version: opts && opts.version
    })
    this._trie.batch([
      { type: 'put', key: p.join(MOUNT_PREFIX, path), hidden: true, value: mountRecord },
      // TODO: empty values going to cause harm here?
      { type: 'put', key: path, flags: Flags.MOUNT, value: Buffer.from('a') }
    ], cb)
  }

  get (path, opts, cb) {
    if (typeof opts === 'function') return this.get(path, null, opts)
    path = normalize(path)

    const self = this

    this._trie.get(path, { ...opts, closest: true }, (err, node) => {
      if (err) return cb(err)
      if (!node) return cb(null, null)
      if (node.flags ^ Flags.MOUNT) return cb(null, node)
      this._trie.get(p.join(MOUNT_PREFIX, path), { hidden: true, closest: true }, getFromMount)
    })

    function getFromMount (err, mountNode) {
      if (err) return cb(err)
      self._trieForMountNode(mountNode, (err, trie, mountInfo) => {
        if (err) return cb(err)
        return trie.get(pathToMount(path, mountInfo), opts, (err, node) => {
          if (err) return cb(err)
          if (!node) return cb(null, null)
          // TODO: do we need to copy the node here?
          node.key = pathFromMount(node.key, mountInfo)
          return cb(null, node)
        })
      })
    }
  }

  put (path, value, opts, cb) {
    if (typeof opts === 'function') return this.put(path, value, null, opts)
    path = normalize(path)

    const self = this
    const condition = putCondition(opts && opts.condition)

    this._trie.put(path, value, { ...opts, condition, closest: true }, (err, inserted) => {
      if (err && !err.mountpoint) return cb(err)
      else if (err) {
        return this._trie.get(p.join(MOUNT_PREFIX, path), { hidden: true, closest: true }, putIntoMount)
      }
      return cb(null, inserted)
    })

    function putIntoMount (err, mountNode) {
      if (err) return cb(err)
      self._trieForMountNode(mountNode, (err, trie, mountInfo) => {
        if (err) return cb(err)
        const mountPath = pathToMount(path, mountInfo)
        return trie.put(mountPath, value, opts, (err, node) => {
          if (err) return cb(err)
          if (!node) return cb(null, null)
          // TODO: do we need to copy the node here?
          node.key = pathFromMount(node.key, mountInfo)
          return cb(null, node)
        })
      })
    }
  }

  del (path, opts, cb) {
    if (isOptions(cb)) return this.del(path, null, opts)
    // TODO: implement
  }

  iterator (prefix, opts) {
    if (isOptions(prefix)) return this.iterator('', prefix)
    if (!prefix) prefix = '/'
    prefix = normalize(prefix)

    const self = this

    // If the iterator contains nodes in the current trie, then the root will be non-null.
    let root = this._trie.iterator(prefix, opts)
    // If the iterator is currently iterating through a sub-trie, then these will be non-null.
    let sub = null
    let subInfo = null

    return nanoiterator({ next })

    function next (cb) {
      if (sub) {
        return sub.next((err, node) => {
          if (err) return cb(err)
          if (!node) {
            sub = subInfo = null
            return next(cb)
          }
          node.key = pathFromMount(node.key, subInfo)
          return cb(null, node)
        })
      }
      root.next((err, node) => {
        if (err) return cb(err)
        if (!node) return cb(null)
        if (node.flags ^ Flags.MOUNT) return cb(null, node)
        self._trie.get(p.join(MOUNT_PREFIX, node.key), { hidden: true, closest: true }, (err, mountNode) => {
          if (err) return cb(err)
          self._trieForMountNode(mountNode, (err, trie, mountInfo) => {
            if (err) return cb(err)
            const subPrefix = pathToMount(node.key, mountInfo)
            sub = trie.iterator(subPrefix, opts)
            subInfo = mountInfo
            return next(cb)
          })
        })
      })
    }
  }

  list (prefix, opts, cb) {
    // Code duplicated from hypertrie.
    if (typeof prefix === 'function') return this.list('', null, prefix)
    if (typeof opts === 'function') return this.list(prefix, null, opts)

    const ite = this.iterator(prefix, opts)
    const res = []

    ite.next(function loop (err, node) {
      if (err) return cb(err)
      if (!node) return cb(null, res)
      res.push(node)
      ite.next(loop)
    })
  }

  batch (ops, cb) {
    // TODO: implement
  }

  checkout (version) {

  }

  snapshot () {

  }

  watch (path, onchange) {

  }

  replicate (opts) {

  }
}

function putCondition (userCondition) {
  return (closest, newNode, cb) => {
    if (closest && (closest.flags & Flags.MOUNT) && (newNode.key.startsWith(closest.key))) {
      const err = new Error('Inserting into mountpoint')
      err.mountpoint = true
      return cb(err)
    }
    if (!userCondition) return cb(null, true)
    userCondition(closest, newNode, (err, shouldPut) => {
      if (err) return cb(err)
      return cb(null, shouldPut)
    })
  }
}

function pathToMount (path, mountInfo) {
  if (path.length === mountInfo.localPath.length) return ''
  return p.join(path.slice(mountInfo.localPath.length), mountInfo.remotePath)
}

function pathFromMount (path, mountInfo) {
  const rel = mountInfo.remotePath ? path.slice(mountInfo.remotePath.length) : path
  return p.join(mountInfo.localPath, rel)
}

function normalize (path) {
  path = unixify(path)
  return path.startsWith('/') ? path.slice(1) :  path
}
