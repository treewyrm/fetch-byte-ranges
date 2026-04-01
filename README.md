# fetch-byte-ranges

HTTP byte-range fetcher.

Issues concurrent partial GETs and returns a `ReadableStream<Uint8Array>` per requested range — no output destination assumed, works anywhere `fetch` does.

```ts
import { fetchByteRanges, collectStream } from '@treewyrm/fetch-byte-ranges'

const { ranges, totalSize } = await fetchByteRanges(url, [
  { start: 0,         end: 1_048_575 }, // first 1 MB
  { start: 2_097_152, end: 3_145_727 }, // third 1 MB
])

// Collect into memory
const buffer = await collectStream(ranges[0].stream)

// Or pipe to a file (Node.js)
import { createWriteStream } from 'node:fs'
import { Writable } from 'node:stream'

ranges[0].stream.pipeTo(Writable.toWeb(createWriteStream('chunkA.bin')))
ranges[1].stream.pipeTo(Writable.toWeb(createWriteStream('chunkB.bin')))

// Or return directly as a Response body (edge/browser)
return new Response(ranges[0].stream)
```

Ranges fed into `fetchByteRanges` are returned as-is in `ranges` property of return value.

## How it works

Every call begins with a preflight `HEAD` to establish the contract:

| Preflight result | Behaviour |
|---|---|
| No `Content-Length` | Throws `MissingContentLengthError` |
| Any range out of bounds | Throws `RangeOutOfBoundsError` |
| `Accept-Ranges: bytes` | Ranged path — concurrent partial GETs |
| Anything else | Full-body path — single GET, ranges sliced locally |

On the ranged path nearby ranges are merged into fewer requests (up to `maxRequests`), their response bodies are tee'd and sliced back into one stream per original range. On the full-body path the single response stream is tee'd and sliced the same way.

## API

### `fetchByteRanges(url, ranges, options?)`

```ts
const { ranges, totalSize } = await fetchByteRanges(url, [
  { start: 0, end: 999 },
])
```

**Options:**

| Option | Type | Default | Description |
|---|---|---|---|
| `concurrency` | `number` | `6` | Max simultaneous range GETs |
| `maxRequests` | `number` | `8` | Max requests after merging nearby ranges |
| `headers` | `HeadersInit` | — | Extra headers on every request |
| `signal` | `AbortSignal` | — | Cancellation signal |
| `fetch` | `typeof fetch` | `globalThis.fetch` | Custom fetch implementation |

**Result:**

| Field | Type | Description |
|---|---|---|
| `ranges` | `RangeResult[]` | One entry per input range, in the same order |
| `totalSize` | `number` | File size from preflight `Content-Length` |

Each `RangeResult`:

| Field | Type | Description |
|---|---|---|
| `range` | `ByteRange` | The requested range |
| `stream` | `ReadableStream<Uint8Array>` | Response bytes — consume exactly once |
| `byteLength` | `number` | `end - start + 1` |
| `contentRange` | `string \| null` | `Content-Range` header; `null` on full-body path |


### `condenseRanges(ranges, maxRanges?)`

Merges overlapping and adjacent ranges, then merges the closest pairs until at most `maxRanges` remain. `fetchByteRages` always runs this function internally.

### `collectStream(stream)`

Convenience helper — drains a `ReadableStream<Uint8Array>` into a `Uint8Array`.

### `splitIntoRanges(totalBytes, count)`

Split a file into `count` evenly-sized `ByteRange`s covering `[0, totalBytes-1]`. Useful for parallel whole-file downloads:

```ts
const { totalSize } = await fetchByteRanges(url, [{ start: 0, end: 0 }])
const chunks = splitIntoRanges(totalSize, 8)
const { ranges } = await fetchByteRanges(url, chunks, { concurrency: 8 })
```

## Errors

| Class | When |
|---|---|
| `MissingContentLengthError` | Preflight HEAD has no `Content-Length` |
| `RangeOutOfBoundsError` | A range's `end` ≥ `totalSize` |
| `RangeNotSatisfiableError` | Server returns `416` |
| `UnexpectedRangeResponseError` | Server returns anything other than `206` on the ranged path |
| `FullBodyTooShortError` | Full-body response closes before covering all requested ranges |

## Byte range semantics

Ranges are **inclusive on both ends**, matching HTTP's `Range` header:

```ts
{ start: 0, end: 9 }   // bytes 0–9 inclusive, 10 bytes total
                       // → Range: bytes=0-9
```

Byte range is a generic type in `fetchByteRanges`.

## Requirements

- Node.js ≥ 18, Deno, Bun, or any modern browser
- No runtime dependencies
