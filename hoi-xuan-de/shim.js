// Stand-in for the claude.ai runtime when Hội Xuân Dê runs as a public website.
// room  -> peer-to-peer presence over WebRTC (Trystero, public Nostr relays for signaling)
// db    -> the altar collection, synced between open browsers (last write per slot wins) and kept in localStorage
// downloads -> a normal browser download
import {joinRoom, selfId} from 'trystero'

const params = new URLSearchParams(location.search)
const roomName = (params.get('phong') || 'main').toLowerCase().replace(/[^a-z0-9_-]/g, '').slice(0, 40) || 'main'
const rtc = joinRoom({appId: 'hoi-xuan-de-2027-tet'}, roomName)

// ---------- room (presence only; the game never emits topics) ----------
const MAX_PRESENCE = 4096
let mine = {}
const others = new Map() // peerId -> {presence, updatedAt}
const peerListeners = new Set(), connListeners = new Set()
let snapshot = Object.freeze([])
let pendingChange = null
const presenceAction = rtc.makeAction('presence')

function peerObj(id, presence, updatedAt, me) {
  return Object.freeze({peer: id, by: null, isMe: me, sameTab: me, kind: 'viewer', guest: false,
    presence: Object.freeze({...presence}), updatedAt})
}
let meObj = peerObj(selfId, {}, Date.now(), true)
const objs = new Map() // peerId -> frozen Peer
function rebuild() {
  snapshot = Object.freeze([meObj, ...objs.values()])
}
function queueChange(kind, p) {
  if (!pendingChange) {
    pendingChange = {joined: [], left: [], updated: []}
    requestAnimationFrame(() => {
      const ch = pendingChange; pendingChange = null
      const out = {peers: snapshot, joined: Object.freeze(ch.joined), left: Object.freeze(ch.left), updated: Object.freeze(ch.updated)}
      peerListeners.forEach(fn => { try { fn(out) } catch (e) { console.error(e) } })
    })
  }
  pendingChange[kind].push(p)
}
function setOther(id, presence) {
  const had = objs.has(id)
  const o = peerObj(id, presence, Date.now(), false)
  objs.set(id, o); rebuild(); queueChange(had ? 'updated' : 'joined', o)
}

// patches are coalesced and sent ~12x a second; a joining peer gets the whole object
let outPatch = null, flushTimer = 0
function flush() {
  flushTimer = 0
  if (!outPatch) return
  const patch = outPatch; outPatch = null
  presenceAction.send({patch}).catch?.(() => {})
}
presenceAction.onMessage = (data, {peerId}) => {
  if (!data || typeof data !== 'object') return
  const cur = others.get(peerId) || {}
  let next
  if (data.full && typeof data.full === 'object') next = {...data.full}
  else if (data.patch && typeof data.patch === 'object') {
    next = {...cur}
    for (const k in data.patch) { if (data.patch[k] === null) delete next[k]; else next[k] = data.patch[k] }
  } else return
  if (JSON.stringify(next).length > MAX_PRESENCE) return
  others.set(peerId, next); setOther(peerId, next)
}
rtc.onPeerJoin = peerId => {
  presenceAction.send({full: mine}, {target: peerId})
  altarAction.send({all: altarState}, {target: peerId})
}
rtc.onPeerLeave = peerId => {
  const o = objs.get(peerId)
  others.delete(peerId); objs.delete(peerId); rebuild()
  if (o) queueChange('left', o)
}

const room = Object.freeze({
  presence(patch) {
    if (!patch || typeof patch !== 'object') return Promise.reject({code: 'invalid_argument', message: 'patch must be an object'})
    const next = {...mine}
    for (const k in patch) { if (patch[k] === null) delete next[k]; else next[k] = patch[k] }
    if (JSON.stringify(next).length > MAX_PRESENCE) return Promise.reject({code: 'invalid_argument', message: 'presence over 4 KiB'})
    mine = next
    meObj = peerObj(selfId, mine, Date.now(), true); rebuild(); queueChange('updated', meObj)
    outPatch = Object.assign(outPatch || {}, patch)
    if (!flushTimer) flushTimer = setTimeout(flush, 80)
    return Promise.resolve()
  },
  peers() { return snapshot },
  onPeers(fn) {
    peerListeners.add(fn)
    queueMicrotask(() => { try { fn({peers: snapshot, joined: snapshot, left: [], updated: []}) } catch (e) { console.error(e) } })
    return () => peerListeners.delete(fn)
  },
  connected() { return true },
  onConnection(fn) { connListeners.add(fn); queueMicrotask(() => fn(true)); return () => connListeners.delete(fn) },
  emit() { return Promise.resolve() },
  on() { return () => {} },
})

// ---------- db: only the "altar" collection is used ----------
const ALTAR_KEY = 'hxd.web.altar.' + roomName
let altarState = {}
try { const a = JSON.parse(localStorage.getItem(ALTAR_KEY) || '{}'); if (a && typeof a === 'object') altarState = a } catch (e) {}
const altarListeners = new Set()
const altarAction = rtc.makeAction('altar')
function saveAltar() { try { localStorage.setItem(ALTAR_KEY, JSON.stringify(altarState)) } catch (e) {} }
function emitAltar() {
  const docs = Object.keys(altarState).map(id => { const body = Object.freeze({...altarState[id]}); return Object.freeze({id, exists: true, data: () => body, metadata: {fromCache: false, hasPendingWrites: false}}) })
  const qs = Object.freeze({docs, size: docs.length, empty: !docs.length, metadata: {fromCache: false, hasPendingWrites: false}, docChanges: () => []})
  altarListeners.forEach(fn => { try { fn(qs) } catch (e) { console.error(e) } })
}
function mergeSlot(id, body) {
  if (!/^[a-zA-Z0-9]{1,12}$/.test(id) || !body || typeof body !== 'object' || typeof body.item !== 'string') return false
  const cur = altarState[id], t = +body.t || 0
  if (cur && (+cur.t || 0) >= t) return false
  altarState[id] = {item: String(body.item).slice(0, 20), by: String(body.by || '').slice(0, 18), t}
  return true
}
altarAction.onMessage = data => {
  if (!data || typeof data !== 'object') return
  let changed = false
  if (data.all && typeof data.all === 'object') for (const id in data.all) changed = mergeSlot(id, data.all[id]) || changed
  if (data.slot) changed = mergeSlot(data.slot, data.body) || changed
  if (changed) { saveAltar(); emitAltar() }
}
const db = Object.freeze({
  collection(path) {
    return {
      onSnapshot(next) { if (path !== 'altar') return () => {}; altarListeners.add(next); queueMicrotask(emitAltar); return () => altarListeners.delete(next) },
      doc(id) { return db.doc(path + '/' + id) },
    }
  },
  doc(path) {
    const [col, id] = path.split('/')
    return {
      set(body) {
        if (col !== 'altar') return Promise.reject({code: 'invalid_argument', message: 'unknown collection'})
        const b = {...body, t: Math.max(+body.t || 0, ((altarState[id] && +altarState[id].t) || 0) + 1)}
        mergeSlot(id, b); saveAltar(); emitAltar()
        altarAction.send({slot: id, body: altarState[id]})
        return Promise.resolve()
      },
    }
  },
})

// ---------- downloads ----------
const downloads = Object.freeze({
  save({filename, data}) {
    const blob = data instanceof Blob ? data : new Blob([data])
    const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = filename
    document.body.appendChild(a); a.click(); a.remove()
    setTimeout(() => URL.revokeObjectURL(a.href), 4000)
    return Promise.resolve({status: 'saved'})
  },
})

const caps = {room, db, downloads}
window.claude = Object.freeze({use: name => Promise.resolve(caps[name] || null)})
window.addEventListener('pagehide', () => { try { rtc.leave() } catch (e) {} })
