import { describe, it } from 'node:test'
import { deepEqual, equal, rejects, throws } from 'node:assert'

import {
  MockAgent,
  fetch as undiciFetch,
  type Interceptable,
  type RequestInfo,
  type RequestInit,
} from 'undici'

import {
  fetchByteRanges,
  condenseRanges,
  collectStream,
  splitIntoRanges,
  MissingContentLengthError,
  RangeOutOfBoundsError,
  RangeNotSatisfiableError,
  UnexpectedRangeResponseError,
  // FullBodyTooShortError,
} from './index.js'

// ─── Helpers ──────────────────────────────────────────────────────────────────

const BASE_URL = 'http://example.com'
const FILE_URL = `${BASE_URL}/file`
const FILE_SIZE = 100

function makeMock() {
  const agent = new MockAgent()
  agent.disableNetConnect()

  const pool = agent.get(BASE_URL)
  const fetch = ((url: RequestInfo, init: RequestInit = {}) =>
    undiciFetch(url, { ...init, dispatcher: agent })) as (typeof globalThis)['fetch']

  return { fetch, pool }
}

function interceptHead(
  pool: Interceptable,
  { status = 200, contentLength = FILE_SIZE, acceptRanges = 'bytes' } = {},
) {
  const headers: HeadersInit = {}

  if (contentLength !== null) headers['Content-Length'] = String(contentLength)
  if (acceptRanges !== null) headers['Accept-Ranges'] = acceptRanges

  pool.intercept({ path: '/file', method: 'HEAD' }).reply(status, '', { headers })
}

function intercept206(
  pool: Interceptable,
  rangeHeader: string,
  data: string | Uint8Array<ArrayBuffer>,
  totalSize = FILE_SIZE,
) {
  const [start, end] = rangeHeader.split('-').map(Number)

  pool
    .intercept({ path: '/file', method: 'GET', headers: { range: `bytes=${rangeHeader}` } })
    .reply(206, Buffer.from(data), {
      headers: {
        'Content-Range': `bytes ${start}-${end}/${totalSize}`,
        'Content-Length': String(data.length),
      },
    })
}

const intercept200 = (pool: Interceptable, data: string | Uint8Array<ArrayBuffer>) =>
  pool
    .intercept({ path: '/file', method: 'GET' })
    .reply(200, Buffer.from(data), { headers: { 'Content-Length': String(data.length) } })

const seq = (start: number, length: number) =>
  Uint8Array.from({ length }, (_, i) => (start + i) % 256)

// ─── condenseRanges ───────────────────────────────────────────────────────────

describe('condenseRanges', () => {
  it('returns empty array for empty input', () => {
    deepEqual(condenseRanges([]), [])
  })

  it('returns single range unchanged', () => {
    deepEqual(condenseRanges([{ start: 10, end: 20 }]), [{ start: 10, end: 20 }])
  })

  it('merges overlapping ranges', () => {
    deepEqual(
      condenseRanges([
        { start: 0, end: 20 },
        { start: 10, end: 30 },
      ]),
      [{ start: 0, end: 30 }],
    )
  })

  it('merges adjacent ranges (inclusive end — no gap)', () => {
    deepEqual(
      condenseRanges([
        { start: 0, end: 9 },
        { start: 10, end: 19 },
      ]),
      [{ start: 0, end: 19 }],
    )
  })

  it('keeps non-adjacent ranges separate when within maxRanges', () => {
    deepEqual(
      condenseRanges(
        [
          { start: 0, end: 9 },
          { start: 20, end: 29 },
        ],
        8,
      ),
      [
        { start: 0, end: 9 },
        { start: 20, end: 29 },
      ],
    )
  })

  it('merges smallest-gap pair when over maxRanges', () => {
    deepEqual(
      condenseRanges(
        [
          { start: 0, end: 9 },
          { start: 20, end: 29 },
          { start: 35, end: 44 },
        ],
        2,
      ),
      [
        { start: 0, end: 9 },
        { start: 20, end: 44 },
      ],
    )
  })

  it('handles wider range absorbing narrower at same start', () => {
    deepEqual(
      condenseRanges([
        { start: 0, end: 50 },
        { start: 0, end: 20 },
      ]),
      [{ start: 0, end: 50 }],
    )
  })

  it('throws when maxRanges < 1', () => {
    throws(() => condenseRanges([{ start: 0, end: 9 }], 0), /maxRanges must be at least 1/)
  })
})

// ─── splitIntoRanges ──────────────────────────────────────────────────────────

describe('splitIntoRanges', () => {
  it('returns empty array for zero bytes or zero count', () => {
    deepEqual(splitIntoRanges(0, 4), [])
    deepEqual(splitIntoRanges(100, 0), [])
  })

  it('covers exactly [0, totalBytes-1] with no gaps or overlaps', () => {
    const ranges = splitIntoRanges(100, 4)

    equal(ranges[0]!.start, 0)
    equal(ranges[ranges.length - 1]!.end, 99)

    for (let i = 1; i < ranges.length; i++) {
      equal(ranges[i]!.start, ranges[i - 1]!.end + 1)
    }
  })

  it('single range for count=1 covers whole file', () => {
    deepEqual(splitIntoRanges(100, 1), [{ start: 0, end: 99 }])
  })

  it('handles uneven split', () => {
    deepEqual(splitIntoRanges(10, 3), [
      { start: 0, end: 3 },
      { start: 4, end: 7 },
      { start: 8, end: 9 },
    ])
  })
})

// ─── collectStream ────────────────────────────────────────────────────────────

describe('collectStream', () => {
  it('collects a single-chunk stream', async () => {
    const data = seq(0, 10)
    const stream = new ReadableStream({
      start(c) {
        c.enqueue(data)
        c.close()
      },
    })

    deepEqual(await collectStream(stream), data)
  })

  it('concatenates multiple chunks', async () => {
    const chunks = [seq(0, 5), seq(5, 5)]
    let i = 0
    const stream = new ReadableStream({
      pull(c) {
        i < chunks.length ? c.enqueue(chunks[i++]) : c.close()
      },
    })

    deepEqual(await collectStream(stream), seq(0, 10))
  })

  it('returns empty Uint8Array for empty stream', async () => {
    const stream = new ReadableStream({
      start(c) {
        c.close()
      },
    })

    equal((await collectStream(stream)).byteLength, 0)
  })
})

// ─── fetchByteRanges — preflight ─────────────────────────────────────────────

describe('fetchByteRanges — preflight', () => {
  it('returns empty result without issuing a HEAD for empty ranges', async () => {
    const { fetch } = makeMock() // disableNetConnect — any request would throw

    const result = await fetchByteRanges(FILE_URL, [], { fetch })

    deepEqual(result.ranges, [])
    equal(result.totalSize, 0)
  })

  it('totalSize is always populated from preflight Content-Length', async () => {
    const { fetch, pool } = makeMock()

    interceptHead(pool, { contentLength: 12345 })
    intercept206(pool, '0-9', seq(0, 10), 12345)

    const { totalSize } = await fetchByteRanges(FILE_URL, [{ start: 0, end: 9 }], { fetch })

    equal(totalSize, 12345)
  })

  it('throws MissingContentLengthError when preflight has no Content-Length', async () => {
    const { fetch, pool } = makeMock()

    // @ts-ignore
    interceptHead(pool, { contentLength: null })

    await rejects(
      () => fetchByteRanges(FILE_URL, [{ start: 0, end: 9 }], { fetch }),
      MissingContentLengthError,
    )
  })

  it('throws RangeOutOfBoundsError when a range exceeds file size', async () => {
    const { fetch, pool } = makeMock()

    interceptHead(pool, { contentLength: 50 })

    await rejects(
      () => fetchByteRanges(FILE_URL, [{ start: 40, end: 59 }], { fetch }),
      RangeOutOfBoundsError,
    )
  })
})

// ─── fetchByteRanges — ranged path ───────────────────────────────────────────

describe('fetchByteRanges — ranged path (Accept-Ranges: bytes)', () => {
  it('fetches a single range and streams correct bytes', async () => {
    const { fetch, pool } = makeMock()

    interceptHead(pool)
    intercept206(pool, '0-9', seq(0, 10))

    const { ranges } = await fetchByteRanges(FILE_URL, [{ start: 0, end: 9 }], { fetch })

    equal(ranges[0]!.byteLength, 10)
    deepEqual(await collectStream(ranges[0]!.stream), seq(0, 10))
  })

  it('sets correct byteLength for single-byte range', async () => {
    const { fetch, pool } = makeMock()

    interceptHead(pool)
    intercept206(pool, '42-42', new Uint8Array([0xab]))

    const { ranges } = await fetchByteRanges(FILE_URL, [{ start: 42, end: 42 }], { fetch })

    equal(ranges[0]!.byteLength, 1)
  })

  it('sends inclusive Range header (bytes=0-9 not bytes=0-10)', async () => {
    const { fetch, pool } = makeMock()

    interceptHead(pool)

    // intercept fires only if Range header matches exactly
    pool
      .intercept({ path: '/file', method: 'GET', headers: { range: 'bytes=0-9' } })
      .reply(206, Buffer.from(seq(0, 10)), {
        headers: { 'Content-Range': `bytes 0-9/${FILE_SIZE}` },
      })
    const { ranges } = await fetchByteRanges(FILE_URL, [{ start: 0, end: 9 }], { fetch })

    await collectStream(ranges[0]!.stream)
  })

  it('fetches two non-overlapping ranges as separate requests', async () => {
    const { fetch, pool } = makeMock()

    interceptHead(pool)
    intercept206(pool, '0-9', seq(0, 10))
    intercept206(pool, '50-59', seq(50, 10))

    const { ranges } = await fetchByteRanges(
      FILE_URL,
      [
        { start: 0, end: 9 },
        { start: 50, end: 59 },
      ],
      { fetch },
    )

    deepEqual(await collectStream(ranges[0]!.stream), seq(0, 10))
    deepEqual(await collectStream(ranges[1]!.stream), seq(50, 10))
  })

  it('preserves original request ranges in output', async () => {
    const { fetch, pool } = makeMock()

    interceptHead(pool)
    intercept206(pool, '0-9', seq(0, 10))
    intercept206(pool, '50-59', seq(50, 10))

    const { ranges } = await fetchByteRanges(
      FILE_URL,
      [
        { start: 0, end: 9, name: 'first' },
        { start: 50, end: 59, name: 'second' },
      ],
      { fetch },
    )

    deepEqual(ranges[0]!.range.name, 'first')
    deepEqual(ranges[1]!.range.name, 'second')
  })

  it('merges adjacent ranges and slices output', async () => {
    const { fetch, pool } = makeMock()

    interceptHead(pool)
    intercept206(pool, '0-19', seq(0, 20))

    const { ranges } = await fetchByteRanges(
      FILE_URL,
      [
        { start: 0, end: 9 },
        { start: 10, end: 19 },
      ],
      { fetch, maxRequests: 1 },
    )

    deepEqual(await collectStream(ranges[0]!.stream), seq(0, 10))
    deepEqual(await collectStream(ranges[1]!.stream), seq(10, 10))
  })

  it('merges overlapping ranges and slices correct windows', async () => {
    const { fetch, pool } = makeMock()

    interceptHead(pool)
    intercept206(pool, '0-19', seq(0, 20))

    const { ranges } = await fetchByteRanges(
      FILE_URL,
      [
        { start: 0, end: 15 },
        { start: 5, end: 19 },
      ],
      { fetch, maxRequests: 1 },
    )

    deepEqual(await collectStream(ranges[0]!.stream), seq(0, 16))
    deepEqual(await collectStream(ranges[1]!.stream), seq(5, 15))
  })

  it('passes extra headers to every GET request', async () => {
    const { fetch, pool } = makeMock()

    interceptHead(pool)

    pool
      .intercept({ path: '/file', method: 'GET', headers: { authorization: 'Bearer tok' } })
      .reply(206, Buffer.from(seq(0, 5)), {
        headers: { 'Content-Range': `bytes 0-4/${FILE_SIZE}` },
      })

    const { ranges } = await fetchByteRanges(FILE_URL, [{ start: 0, end: 4 }], {
      fetch,
      headers: { Authorization: 'Bearer tok' },
    })

    await collectStream(ranges[0]!.stream)
  })

  it('preserves result order regardless of concurrency', async () => {
    const { fetch, pool } = makeMock()

    interceptHead(pool)
    intercept206(pool, '0-9', seq(0, 10))
    intercept206(pool, '20-29', seq(20, 10))
    intercept206(pool, '40-49', seq(40, 10))

    const { ranges } = await fetchByteRanges(
      FILE_URL,
      [
        { start: 0, end: 9 },
        { start: 20, end: 29 },
        { start: 40, end: 49 },
      ],
      { fetch, concurrency: 3 },
    )

    deepEqual(await collectStream(ranges[0]!.stream), seq(0, 10))
    deepEqual(await collectStream(ranges[1]!.stream), seq(20, 10))
    deepEqual(await collectStream(ranges[2]!.stream), seq(40, 10))
  })

  it('throws RangeNotSatisfiableError on 416', async () => {
    const { fetch, pool } = makeMock()

    interceptHead(pool)

    pool.intercept({ path: '/file', method: 'GET' }).reply(416, '')

    await rejects(
      () => fetchByteRanges(FILE_URL, [{ start: 0, end: 9 }], { fetch }),
      RangeNotSatisfiableError,
    )
  })

  it('throws UnexpectedRangeResponseError when server returns 200 on a ranged GET', async () => {
    const { fetch, pool } = makeMock()

    interceptHead(pool) // declares Accept-Ranges: bytes

    pool.intercept({ path: '/file', method: 'GET' }).reply(200, Buffer.from(seq(0, FILE_SIZE)))

    await rejects(
      () => fetchByteRanges(FILE_URL, [{ start: 0, end: 9 }], { fetch }),
      UnexpectedRangeResponseError,
    )
  })

  it('throws UnexpectedRangeResponseError on other unexpected status codes', async () => {
    const { fetch, pool } = makeMock()

    interceptHead(pool)

    pool.intercept({ path: '/file', method: 'GET' }).reply(503, 'unavailable')

    await rejects(
      () => fetchByteRanges(FILE_URL, [{ start: 0, end: 9 }], { fetch }),
      UnexpectedRangeResponseError,
    )
  })

  it('propagates AbortError when signal fires', async () => {
    const { fetch, pool } = makeMock()

    interceptHead(pool)

    const controller = new AbortController()

    controller.abort()

    await rejects(
      () => fetchByteRanges(FILE_URL, [{ start: 0, end: 9 }], { fetch, signal: controller.signal }),
      { name: 'AbortError' },
    )
  })
})

// ─── fetchByteRanges — full-body path ────────────────────────────────────────

describe('fetchByteRanges — full-body path (no Accept-Ranges: bytes)', () => {
  it('uses full-body path when Accept-Ranges: none', async () => {
    const { fetch, pool } = makeMock()

    interceptHead(pool, { acceptRanges: 'none' })
    intercept200(pool, seq(0, FILE_SIZE))

    const { ranges, totalSize } = await fetchByteRanges(
      FILE_URL,
      [
        { start: 10, end: 19 },
        { start: 40, end: 49 },
      ],
      { fetch },
    )

    equal(totalSize, FILE_SIZE)
    equal(ranges[0]!.contentRange, null)
    deepEqual(await collectStream(ranges[0]!.stream), seq(10, 10))
    deepEqual(await collectStream(ranges[1]!.stream), seq(40, 10))
  })

  it('uses full-body path when Accept-Ranges header is absent', async () => {
    const { fetch, pool } = makeMock()

    // @ts-ignore
    interceptHead(pool, { acceptRanges: null })
    intercept200(pool, seq(0, FILE_SIZE))

    const { ranges } = await fetchByteRanges(FILE_URL, [{ start: 0, end: 9 }], { fetch })

    deepEqual(await collectStream(ranges[0]!.stream), seq(0, 10))
  })

  it('uses full-body path when Accept-Ranges has any value other than "bytes"', async () => {
    const { fetch, pool } = makeMock()

    interceptHead(pool, { acceptRanges: 'none' })
    intercept200(pool, seq(0, FILE_SIZE))

    const { ranges } = await fetchByteRanges(FILE_URL, [{ start: 0, end: 9 }], { fetch })

    deepEqual(await collectStream(ranges[0]!.stream), seq(0, 10))
  })

  it('errors stream when body is too short for the requested range', async () => {
    const { fetch, pool } = makeMock()

    interceptHead(pool, { acceptRanges: 'none', contentLength: 100 })
    // Deliver only 50 bytes despite Content-Length: 100

    pool.intercept({ path: '/file', method: 'GET' }).reply(200, Buffer.from(seq(0, 50)))

    const { ranges } = await fetchByteRanges(FILE_URL, [{ start: 60, end: 79 }], { fetch })

    await rejects(() => collectStream(ranges[0]!.stream), { name: 'FullBodyTooShortError' })
  })
})

// ─── fetchByteRanges — input validation ──────────────────────────────────────

describe('fetchByteRanges — input validation', () => {
  it('throws RangeError for start > end', async () => {
    const { fetch } = makeMock()

    await rejects(() => fetchByteRanges(FILE_URL, [{ start: 10, end: 5 }], { fetch }), RangeError)
  })

  it('throws RangeError for negative start', async () => {
    const { fetch } = makeMock()

    await rejects(() => fetchByteRanges(FILE_URL, [{ start: -1, end: 9 }], { fetch }), RangeError)
  })
})
