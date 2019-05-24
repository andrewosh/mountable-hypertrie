# mountable-hypertrie
[![Build Status](https://travis-ci.com/andrewosh/mountable-hypertrie.svg?token=WgJmQm3Kc6qzq1pzYrkx&branch=master)](https://travis-ci.com/andrewosh/mountable-hypertrie)

A Hypertrie wrapper that supports mounting of sub-Hypertries.

### Usage

### API
`mountable-hypertrie` re-exposes the [`hypertrie`](https://github.com/mafintosh/hypertrie) API, with the addition of the following methods (and a different constructor):

_Note: We're still adding support for many hypertrie methods. Here's what's been implemented so far:_

- [ ] `get`
- [ ] `put`
- [ ] `batch`
- [ ] `iterator`
- [ ] `list`
- [ ] `createReadStream`
- [ ] `createWriteStream`
- [ ] `checkout`
- [ ] `watch`
- [ ] `createHistoryStream`
- [ ] `createDiffStream`

#### `const trie = new MountableHypertrie(corestore, key, opts)`
`corestore` can be any object that implements the corestore interface. For now, it's recommanded to use [`random-access-corestore`](https://github.com/andrewosh/random-access-corestore)
`key` is the hypertrie key
`opts` can contain any `hypertrie` options

#### `trie.mount(path, key, opts, cb)`
#### `trie.unmount(path, cb)`

### License
MIT
