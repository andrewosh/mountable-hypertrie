const p = require('path')
const hypertrie = require('hypertrie')
const thunky = require('thunky')
const nanoiterator = require('nanoiterator')
const toStream = require('nanoiterator/to-stream')
const isOptions = require('is-options')
const unixify = require('unixify')

const { Mount } = require('./lib/messages')

const Flags = {
  MOUNT: 1
}
const MOUNT_PREFIX = '/mounts'

class MountableHypertrie {
  constructor (corestore, key, opts = {}) {
    this.corestore = corestore
    this.key = key
    this.opts = opts

    // Set in _ready.
    this._trie = (opts && opts.trie) || hypertrie(null, {
      feed: this.opts.feed || this.corestore.get({ key: this.key, main: !this.key, ...this.opts }),
      ...opts
    })
    // TODO: Replace with a LRU cache.
    this._tries = new Map()
    this._checkouts = new Map()

    this.ready = thunky(this._ready.bind(this))
  }

  _ready (cb) {
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

    var trie = this._tries.get(key) || new MountableHypertrie(this.corestore, key, opts)
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

  get version () {
    return this._trie.version
  }

  getMetadata (cb) {
    return this._trie.getMetadata(cb)
  }

  setMetadata (metadata, cb) {
    return this._trie.setMetadata(metadata, cb)
  }

  getFeed () {
    if (!this._trie) return null
    return this._trie.feed
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
      { type: 'put', key: path, flags: Flags.MOUNT, value: (opts && opts.value) || Buffer.alloc(0) }
    ], cb)
  }

  get (path, opts, cb) {
    if (typeof opts === 'function') return this.get(path, null, opts)
    path = normalize(path)

    const self = this

    this._trie.get(path, { ...opts, closest: true }, (err, node) => {
      if (err) return cb(err)
      if (!node) return cb(null, null, this)
      if (node.flags ^ Flags.MOUNT) {
        if (node.key !== path) return cb(null, null, this)
        node[MountableHypertrie.Symbols.TRIE] = this
        return cb(null, node, this)
      }
      if (node.key === path) return cb(null, node)
      this._trie.get(p.join(MOUNT_PREFIX, path), { hidden: true, closest: true }, getFromMount)
    })

    function getFromMount (err, mountNode) {
      if (err) return cb(err)
      self._trieForMountNode(mountNode, (err, trie, mountInfo) => {
        if (err) return cb(err)
        return trie.get(pathToMount(path, mountInfo), opts, (err, node, subTrie) => {
          if (err) return cb(err)
          if (!node) return cb(null, null, subTrie)
          // TODO: do we need to copy the node here?
          node.key = pathFromMount(node.key, mountInfo)
          if (node.key !== path) return cb(null, null, subTrie)
          if (!node[MountableHypertrie.Symbols.TRIE]) node[MountableHypertrie.Symbols.TRIE] = subTrie
          return cb(null, node, subTrie)
        })
      })
    }
  }

  put (path, value, opts, cb) {
    if (typeof opts === 'function') return this.put(path, value, null, opts)
    path = normalize(path)

    const self = this
    const condition = putCondition(path, opts)

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

  // TODO: remove duplicate code
  del (path, opts, cb) {
    if (typeof opts === 'function') return this.del(path, null, opts)
    path = normalize(path)

    const self = this
    const condition = delCondition(path, opts && opts.condition)
  
    console.log('TOP LEVEL DELETE, path:', path)
    this._trie.del(path, { ...opts, condition, closest: true }, (err, deleted) => {
      if (err && !err.mountpoint) return cb(err)
      else if (err) {
        console.log('DELETING FROM A SUBTRIE')
        return this._trie.get(p.join(MOUNT_PREFIX, path), { hidden: true, closest: true }, delFromMount)
      }
      console.log('DELETED FROM MY TRIE, deleted:', deleted)
      return cb(null, deleted)
    })

    function delFromMount (err, mountNode) {
      if (err) return cb(err)
      self._trieForMountNode(mountNode, (err, trie, mountInfo) => {
        if (err) return cb(err)
        const mountPath = pathToMount(path, mountInfo)
        console.log('DELETING AT MOUNTPATH:', mountPath, 'path:', path, 'mountInfo:', mountInfo)
        return trie.del(mountPath, opts, (err, node) => {
          if (err) return cb(err)
          console.log('AFTER DEL, node:', node)
          if (!node) return cb(null, null)
          // TODO: do we need to copy the node here?
          node.key = pathFromMount(node.key, mountInfo)
          return cb(null, node)
        })
      })
    }
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
          if (!node[MountableHypertrie.Symbols.TRIE]) node[MountableHypertrie.Symbols.TRIE] = sub
          return cb(null, node)
        })
      }
      root.next((err, node) => {
        if (err) return cb(err)
        if (!node) return cb(null, null)

        node[MountableHypertrie.Symbols.TRIE] = self
        if (node.flags ^ Flags.MOUNT) return cb(null, node)

        self._trie.get(p.join(MOUNT_PREFIX, node.key), { hidden: true, closest: true }, (err, mountNode) => {
          if (err) return cb(err)
          self._trieForMountNode(mountNode, (err, trie, mountInfo) => {
            if (err) return cb(err)
            const subPrefix = pathToMount(node.key, mountInfo)
            sub = trie.iterator(subPrefix, opts)
            subInfo = mountInfo
            return cb(null, node)
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

  createReadStream (prefix, opts) {
    return toStream(this.iterator(prefix, opts))
  }

  batch (ops, cb) {
    // TODO: implement
  }

  checkout (version) {
    if (version === 0) version = 1
    return new MountableHypertrie(this.corestore, null, {
      trie: this._trie,
      checkout: version || 1,
      ...this.opts
    })
  }

  watch (path, onchange) {
    let rootWatcher = this._trie.watch(path, onchange)
    const watchers = []
    this._trie.list(p.join(MOUNT_PREFIX, path), { hidden: true }, (err, mountNodes) => {
      if (err) return rootWatcher.emit('error', err)
      for (let mountNode of mountNodes) {
        this._trieForMountNode(mountNode, (err, trie, mountInfo) => {
          if (err) return rootWatcher.emit('error', err)
          watchers.push(trie.watch(pathToMount(path, mountInfo), onchange))
        })
      }
    })
    const destroy = rootWatcher.destroy.bind(rootWatcher)
    rootWatcher.destroy = function () {
      destroy()
      for (let watcher of watcherss) {
        watcher.destroy()
      }
    }
    return rootWatcher
  }

  replicate (opts) {
    return this.corestore.replicate(opts)
  }
}

MountableHypertrie.Symbols = MountableHypertrie.prototype.Symbols = {
  TRIE: Symbol('trie')
}

module.exports = MountableHypertrie

function putCondition (path, opts) {
  const userCondition = opts && opts.condition
  const userClosest = opts && opts.closest
  return (closest, newNode, cb) => {
    if (closest && (closest.flags & Flags.MOUNT) && newNode.key.startsWith(closest.key)) {
      const err = new Error('Operating on a mountpoint')
      err.mountpoint = true
      return cb(err)
    }
    if (!userCondition) return cb(null, true)
    if (closest && closest.key !== newNode.key && !userClosest) closest = null 
    userCondition(closest, newNode, (err, shouldExecute) => {
      if (err) return cb(err)
    console.log('SHOULD EXECUTE:', shouldExecute)
      return cb(null, shouldExecute)
    })
  }
}

function delCondition (path, userCondition) {
  return (closest, cb) => {
    console.log('IN DEL CONDITION, closest:', closest)
    if (closest && (closest.flags & Flags.MOUNT) && (closest.key !== path)) {
      const err = new Error('Operating on a mountpoint')
      err.mountpoint = true
      console.log('RETURNING MOUTNPOINT ERR')
      return cb(err)
    }
    console.log('RETURNING TRUE IN DEL CONDITION')
    if (!userCondition) return cb(null, true)
    userCondition(closest, (err, shouldExecute) => {
      if (err) return cb(err)
      return cb(null, shouldExecute)
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
