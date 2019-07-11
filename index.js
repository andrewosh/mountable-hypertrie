const p = require('path').posix
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
const OWNER = Symbol('mountable-hypertrie-owner')

class MountableHypertrie {
  constructor (corestore, key, opts = {}) {
    this.corestore = corestore
    this.key = key
    this.opts = opts
    this.sparse = opts.sparse !== false

    if (opts.valueEncoding) throw new Error('MountableHypertrie does not currently support the valueEncoding opt.')

    var feed = this.opts.feed
    if (!feed) {
      if (!opts.secretKey) feed = this.corestore.default({ key, ...this.opts })
      feed = this.corestore.get({ key, discoverable: true, ...this.opts })
    }
    this._trie = opts.trie || hypertrie(null, {
      ...opts,
      feed
    })
    if (opts.version) this._trie = this._trie.checkout(opts.version)
    if (!opts.version) {
      // If this is a checkout, it will never be writable.
      // If this trie's feed was instantiated by another hypertrie, reuse it here.
      if (this._trie.feed[OWNER]) this._trie = this._trie.feed[OWNER]
      else this._trie.feed[OWNER] = this._trie
    }

    // TODO: Replace with a LRU cache.
    this._tries = new Map()
    this._checkouts = new Map()

    this.ready = thunky(this._ready.bind(this))
  }

  _ready (cb) {
    this._trieReady(this._trie, true, err => {
      if (err) return cb(err)
      this.key = this._trie.key
      return cb(null)
    })
  }

  _trieReady (trie, root, cb) {
    const self = this

    trie.ready(err => {
      if (err) return cb(err)
      update(trie.feed)
    })

    function update (feed) {
      if (self.sparse) {
        // TODO: This is a hack that should be moved into hypercore
        feed.update({ hash: false }, function loop () {
          feed.update(loop)
        })
      }
      if (feed.length !== 0) return cb(null)
      return feed.update({ hash: false, ifAvailable: true }, () => {
        return cb(null)
      })
    }
  }

  _createHypertrie (key, opts, cb) {
    const self = this

    var versionedTrie = (opts && opts.version) ? this._checkouts.get(`${key}:${opts.version}`) : null
    if (versionedTrie) return process.nextTick(cb, null, versionedTrie)

    const keyString = key.toString('hex')
    const subfeed = this.corestore.get({ ...opts, key, discoverable: true })

    var trie = this._tries.get(keyString)
    if (opts && opts.cached) return cb(null, trie)

    trie = trie || new MountableHypertrie(this.corestore, key, {
      ...this.opts,
      ...opts,
      feed: subfeed,
      sparse: this.sparse
    })
    self._tries.set(keyString, trie)

    if (!trie.opened) {
      trie.ready(err => {
        if (err) return cb(err)
        return onready()
      })
    } else process.nextTick(onready)

    function onready () {
      if (!opts || !opts.version) return ontrie(trie)
      versionedTrie = trie.checkout(opts.version)
      this._checkouts.set(`${keyString}:${opts.version}`, versionedTrie)
      return ontrie(versionedTrie)
    }

    function ontrie (trie) {
      self._trieReady(trie._trie, false, err => {
        if (err) return cb(err)
        return cb(null, trie)
      })
    }
  }

  _trieForMountNode (mountNode, opts, cb) {
    if (typeof opts === 'function') return this._trieForMountNode(mountNode, {}, opts)
    opts = opts || {}

    if (!mountNode) return cb(new Error(`Mount metadata not found`))
    try {
      var mountInfo = Mount.decode(mountNode.value)
    } catch (err) {
      return cb(err)
    }

    this._createHypertrie(mountInfo.key, { ...opts, version: mountInfo.version }, (err, trie) => {
      if (err) return cb(err)
      return cb(null, trie, mountInfo)
    })
  }

  _isNormalNode (node) {
    if (!node) return true
    return node.flags ^ Flags.MOUNT
  }

  _getSubtrie (path, cb) {
    this._trie.get(p.join(MOUNT_PREFIX, path), { hidden: true, closest: true }, (err, mountNode) => {
      if (err) return cb(err)
      const mountPath = mountNode && mountNode.key.slice(7)
      if (this._isNormalNode(mountNode) || p.relative(mountPath, path).startsWith('..')) {
        return cb(null, this._trie, { localPath: '', remotePath: '' })
      }
      return this._trieForMountNode(mountNode, cb)
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

    this._getSubtrie(path, (err, trie, mountInfo) => {
      if (err) return cb(err)
      const innerPath = pathToMount(path, mountInfo)
      if (!mountInfo.localPath) {
        return trie.batch([
          { type: 'put', key: p.join(MOUNT_PREFIX, innerPath), flags: Flags.MOUNT, hidden: true, value: mountRecord },
          // TODO: empty values going to cause harm here?
          { type: 'put', key: innerPath, flags: Flags.MOUNT, value: (opts && opts.value) || Buffer.alloc(0) }
        ], cb)
      }
      return trie.mount(innerPath, key, opts, cb)
    })
  }

  unmount (path, cb) {
    path = normalize(path)

    return this._getSubtrie(p.dirname(path), (err, trie, mountInfo) => {
      if (err) return cb(err)
      const innerPath = pathToMount(path, mountInfo)
      return trie.batch([
        { type: 'del', key: p.join(MOUNT_PREFIX, innerPath), hidden: true },
        { type: 'del', key: innerPath }
      ], cb)
    })
  }

  loadMount (path, cb) {
    return this._getSubtrie(path, cb)
  }

  get (path, opts, cb) {
    if (typeof opts === 'function') return this.get(path, null, opts)
    path = normalize(path)

    const self = this

    this._trie.get(path, { ...opts, closest: true }, (err, node) => {
      if (err) return cb(err)
      if (!node) return cb(null, null, this)
      if (this._isNormalNode(node)) {
        if (node.key !== path) return cb(null, null, this)
        node[MountableHypertrie.Symbols.TRIE] = this
        return cb(null, node, this)
      }
      if (node.key === path) return cb(null, node)
      return this._getSubtrie(path, getFromMount)
    })

    function getFromMount (err, trie, mountInfo) {
      if (err) return cb(err)
      return trie.get(pathToMount(path, mountInfo), opts, (err, node, subTrie) => {
        if (err) return cb(err)
        subTrie = subTrie || self
        if (!node) return cb(null, null, subTrie)
        // TODO: do we need to copy the node here?
        node.key = pathFromMount(node.key, mountInfo)
        if (node.key !== path) return cb(null, null, subTrie)
        if (!node[MountableHypertrie.Symbols.TRIE]) node[MountableHypertrie.Symbols.TRIE] = subTrie
        return cb(null, node, subTrie)
      })
    }
  }

  put (path, value, opts, cb) {
    if (typeof opts === 'function') return this.put(path, value, null, opts)
    path = normalize(path)

    const condition = putCondition(path, opts)

    this._trie.put(path, value, { ...opts, condition, closest: true }, (err, inserted) => {
      if (err && !err.mountpoint) return cb(err)
      else if (err) {
        return this._getSubtrie(path, putIntoMount)
      }
      return cb(null, inserted)
    })

    function putIntoMount (err, trie, mountInfo) {
      if (err) return cb(err)
      const mountPath = pathToMount(path, mountInfo)
      return trie.put(mountPath, value, opts, (err, node) => {
        if (err) return cb(err)
        if (!node) return cb(null, null)
        // TODO: do we need to copy the node here?
        node.key = pathFromMount(node.key, mountInfo)
        return cb(null, node)
      })
    }
  }

  // TODO: remove duplicate code
  del (path, opts, cb) {
    if (typeof opts === 'function') return this.del(path, null, opts)
    path = normalize(path)

    const condition = delCondition(path, opts && opts.condition)

    this._trie.del(path, { ...opts, condition, closest: true }, (err, deleted) => {
      if (err && !err.mountpoint) return cb(err)
      else if (err) {
        return this._getSubtrie(path, delFromMount)
      }
      return cb(null, deleted)
    })

    function delFromMount (err, trie, mountInfo) {
      if (err) return cb(err)
      const mountPath = pathToMount(path, mountInfo)
      return trie.del(mountPath, opts, (err, node) => {
        if (err) return cb(err)
        if (!node) return cb(null, null)
        // TODO: do we need to copy the node here?
        node.key = pathFromMount(node.key, mountInfo)
        return cb(null, node)
      })
    }
  }

  iterator (prefix, opts) {
    if (isOptions(prefix)) return this.iterator('', prefix)
    if (!prefix) prefix = '/'
    prefix = normalize(prefix)
    const self = this

    const recursive = !!(opts && opts.recursive)
    const gt = !!(opts && opts.gt)
    // gt must always be false in the trie iteration in order to discover mountpoints.
    if (gt) opts = { ...opts, gt: false }

    // Set in open.
    let root = null
    let rootInfo = null

    // If the iterator is currently iterating through a sub-trie, then these will be non-null.
    let sub = null
    let subInfo = null

    return nanoiterator({ next, open })

    function open (cb) {
      self._getSubtrie(prefix, (err, trie, mountInfo) => {
        if (err) return cb(err)
        const subPrefix = pathToMount(prefix, mountInfo)
        root = trie.iterator(subPrefix, opts)
        rootInfo = mountInfo
        return cb(null)
      })
    }

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
          return prereturn(node, cb)
        })
      }
      root.next((err, node) => {
        if (err) return cb(err)
        if (!node) return cb(null, null)

        node[MountableHypertrie.Symbols.TRIE] = self
        if (self._isNormalNode(node)) return prereturn(node, cb)
        else if (!recursive && node.key !== prefix) return prereturn(node, cb)

        self._getSubtrie(node.key, (err, trie, mountInfo) => {
          if (err) return cb(err)
          const subPrefix = pathToMount(node.key, mountInfo)
          sub = trie.iterator(subPrefix, opts)
          subInfo = mountInfo
          return prereturn(node, cb)
        })
      })
    }

    function prereturn (node, cb) {
      if (gt && node.key === prefix) return next(cb)
      node.key = pathFromMount(node.key, rootInfo)
      return cb(null, node)
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

  mountIterator (opts) {
    const memory = opts && !!opts.memory
    const recursive = opts && !!opts.recursive

    const ite = this._trie.iterator(MOUNT_PREFIX, { hidden: true })
    const stack = [{ trie: this, ite, prefix: '/' }]

    return nanoiterator({ next })

    function next (cb) {
      const { trie, ite, prefix } = stack[0]
      return ite.next((err, mountNode) => {
        if (err) return cb(err)

        if (!mountNode && stack.length === 1) return cb(null)
        if (!mountNode) {
          stack.shift()
          return next(cb)
        }

        trie._trieForMountNode(mountNode, { cached: memory }, (err, subTrie, mountInfo) => {
          if (err) return cb(err)
          if (!subTrie) return next(cb)

          const mountPath = p.join(prefix, mountInfo.localPath)
          if (recursive) {
            stack.unshift({
              prefix: p.join(mountPath, mountInfo.remotePath),
              ite: subTrie.iterator(MOUNT_PREFIX, { hidden: true }),
              trie: subTrie
            })
          }

          return cb(null, {
            path: mountPath,
            trie: subTrie
          })
        })
      })
    }
  }

  listMounts (opts, cb) {
    if (typeof opts === 'function') return this.listMounts(null, opts)
    const vals = []
    const ite = this.mountIterator(opts)
    ite.next(function onnext (err, val) {
      if (err) return cb(err)
      if (!val) return cb(null, vals)
      vals.push(val)
      return ite.next(onnext)
    })
  }

  createReadStream (prefix, opts) {
    return toStream(this.iterator(prefix, opts))
  }

  batch (ops, cb) {
    // TODO: implement
  }

  checkout (version) {
    return new MountableHypertrie(this.corestore, null, {
      trie: this._trie,
      version: version || 1,
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
      for (let watcher of watchers) {
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
      return cb(null, shouldExecute)
    })
  }
}

function delCondition (path, userCondition) {
  return (closest, cb) => {
    if (closest && (closest.flags & Flags.MOUNT) && (closest.key !== path)) {
      const err = new Error('Operating on a mountpoint')
      err.mountpoint = true
      return cb(err)
    }
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
  return path.startsWith('/') ? path.slice(1) : path
}
