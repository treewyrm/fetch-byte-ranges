/**
 * An inclusive byte range [start, end]. Both ends are inclusive, matching
 * HTTP's Range header semantics (e.g. `bytes=0-499` covers 500 bytes).
 * Both `start` and `end` must be non-negative integers with start <= end.
 */
export interface ByteRange {
  start: number

  end: number
}

export interface RangeResult<T extends ByteRange> {
  /** The originally requested range, as passed in by the caller. */
  range: T

  /**
   * Actual byte range served, as reported by the Content-Range response header.
   * `null` on the full-body path — the server returned the whole file.
   */
  contentRange: string | null

  /** Size of this range in bytes (= end - start + 1). */
  byteLength: number

  /** The response body bytes for this range as a stream. Consume exactly once. */
  stream: ReadableStream<Uint8Array>
}

export interface FetchByteRangesOptions {
  /**
   * Maximum number of condensed HTTP requests to fire concurrently.
   * @default 6
   */
  concurrency?: number

  /**
   * Maximum number of HTTP requests after condensing. Adjacent ranges with
   * smaller gaps get merged first until this limit is reached.
   * @default 8
   */
  maxRequests?: number

  /** Additional headers merged into every request (HEAD and GETs). */
  headers?: HeadersInit

  /** AbortSignal to cancel all in-flight requests. */
  signal?: AbortSignal

  /** Custom fetch implementation. Defaults to the global `fetch`. */
  fetch?: typeof fetch
}

export interface FetchByteRangesResult<T extends ByteRange> {
  /** Ordered array matching the input ranges exactly, one entry per requested range. */
  ranges: RangeResult<T>[]

  /** Total file size in bytes, always populated from the preflight Content-Length. */
  totalSize: number
}

// ─── Errors ───────────────────────────────────────────────────────────────────

export class MissingContentLengthError extends Error {
  constructor(public readonly url: string) {
    super(`Preflight HEAD ${url} did not return a Content-Length header`)
    this.name = 'MissingContentLengthError'
  }
}

export class RangeOutOfBoundsError extends Error {
  constructor(
    public readonly range: ByteRange,
    public readonly totalSize: number,
  ) {
    super(`Range [${range.start}, ${range.end}] is out of bounds for file of ${totalSize} bytes`)
    this.name = 'RangeOutOfBoundsError'
  }
}

export class RangeNotSatisfiableError extends Error {
  constructor(
    public readonly range: ByteRange,
    public readonly status: number,
  ) {
    super(`Server returned ${status} for range ${formatRange(range)}`)
    this.name = 'RangeNotSatisfiableError'
  }
}

export class UnexpectedRangeResponseError extends Error {
  constructor(
    public readonly range: ByteRange,
    public readonly status: number,
  ) {
    super(
      `Expected 206 for range ${formatRange(range)} but got ${status}. ` +
        `Server declared Accept-Ranges: bytes in preflight but did not honour it.`,
    )
    this.name = 'UnexpectedRangeResponseError'
  }
}

export class FullBodyTooShortError extends Error {
  constructor(
    public readonly received: number,
    public readonly required: number,
  ) {
    super(`Full-body response too short: received ${received} bytes, need at least ${required}`)
    this.name = 'FullBodyTooShortError'
  }
}

/**
 * Fetch multiple byte ranges from a URL concurrently.
 *
 * Begins with a preflight HEAD request:
 *   - Throws `MissingContentLengthError` if Content-Length is absent.
 *   - Throws `RangeOutOfBoundsError` if any range falls outside the file.
 *   - Proceeds via ranged GETs if the server reports `Accept-Ranges: bytes`.
 *   - Falls back to a single full-body GET otherwise.
 *
 * @example
 * const { ranges, totalSize } = await fetchByteRanges(url, [{ start: 0, end: 1_048_575 }])
 * const buffer = await collectStream(ranges[0].stream)
 */
export async function fetchByteRanges<T extends ByteRange>(
  url: string,
  ranges: T[],
  options: FetchByteRangesOptions = {},
): Promise<FetchByteRangesResult<T>> {
  if (ranges.length === 0) return { ranges: [], totalSize: 0 }

  assertValidRanges(ranges)

  const opts = resolveOptions(options)
  const { totalSize, rangesSupported } = await preflight(url, opts)

  assertRangesInBounds(ranges, totalSize)

  return rangesSupported
    ? handleRanged(url, ranges, totalSize, opts)
    : handleFullBody(url, ranges, totalSize, opts)
}

/**
 * Condense an array of byte ranges into at most `maxRanges` ranges by merging
 * overlapping/adjacent ranges, then iteratively merging the pair with the
 * smallest gap until the count is within limit.
 */
export function condenseRanges(ranges: ByteRange[], maxRanges = 8): ByteRange[] {
  if (maxRanges < 1) throw new Error('maxRanges must be at least 1')
  if (ranges.length === 0) return []

  const sorted = [...ranges].sort((a, b) => a.start - b.start || b.end - a.end)

  // Merge overlapping / adjacent ranges.
  // With inclusive end, [0,9] and [10,19] are adjacent (no gap).
  // sorted is non-empty (guarded above); spread to satisfy noUncheckedIndexedAccess
  const merged: ByteRange[] = [{ ...sorted[0]! }]
  for (let i = 1; i < sorted.length; i++) {
    const top = merged[merged.length - 1]!
    const current = sorted[i]!

    if (current.start <= top.end + 1) {
      top.end = Math.max(top.end, current.end)
    } else {
      merged.push({ ...current })
    }
  }

  // Iteratively merge the pair of neighbours with the smallest gap.
  while (merged.length > maxRanges) {
    let minGap = Infinity
    let minIndex = 0
    
    for (let i = 0; i < merged.length - 1; i++) {
      const gap = merged[i + 1]!.start - merged[i]!.end - 1
      
      if (gap < minGap) {
        minGap = gap
        minIndex = i
      }
    }
    
    merged.splice(minIndex, 2, {
      start: merged[minIndex]!.start,
      end: merged[minIndex + 1]!.end,
    })
  }

  return merged
}

/** Collect a ReadableStream<Uint8Array> into a single Uint8Array. */
export async function collectStream(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const chunks: Uint8Array[] = []
  const reader = stream.getReader()

  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      chunks.push(value)
    }
  } finally {
    reader.releaseLock()
  }

  if (chunks.length === 1) return chunks[0]!

  const totalLength = chunks.reduce((total, { byteLength }) => total + byteLength, 0)
  const result = new Uint8Array(totalLength)

  let offset = 0

  for (const chunk of chunks) {
    result.set(chunk, offset)
    offset += chunk.byteLength
  }
  
  return result
}

/**
 * Split a file into N evenly-sized ranges covering [0, totalBytes-1].
 * Pair with `fetchByteRanges` for parallel download of an entire file.
 */
export function splitIntoRanges(totalBytes: number, count: number): ByteRange[] {
  if (count <= 0 || totalBytes <= 0) return []
  
  const chunkSize = Math.ceil(totalBytes / count)
  const ranges: ByteRange[] = []

  for (let i = 0; i < count; i++) {
    const start = i * chunkSize
    if (start >= totalBytes) break

    ranges.push({
      start,
      end: Math.min(start + chunkSize - 1, totalBytes - 1),
    })
  }

  return ranges
}

/**
 * Full-body path — server did not declare Accept-Ranges: bytes.
 * Issues a single GET and slices each requested range out of the body locally.
 */
async function handleFullBody <T extends ByteRange>(
  url: string,
  ranges: T[],
  totalSize: number,
  opts: ResolvedOptions,
): Promise<FetchByteRangesResult<T>> {
  const response = await opts.fetch(url, {
    headers: new Headers(opts.headers),
    signal: opts.signal,
  })

  if (!response.ok) {
    const body = await response.text().catch(() => '')
    throw new Error(`Full-body GET ${url} returned ${response.status}: ${body}`)
  }

  const minRequired = Math.max(...ranges.map((r) => r.end)) + 1
  const guardedBody = guardStream(response.body ?? emptyStream(), minRequired)

  const plan: RangePlanEntry[] = ranges.map((r) => ({
    condensedRange: { start: 0, end: minRequired - 1 },
    offsetInCondensed: r.start,
  }))

  return {
    totalSize,
    ranges: distributeStream(guardedBody, ranges, plan).map((stream, i) => ({
      range: ranges[i]!,
      contentRange: null,
      byteLength: ranges[i]!.end - ranges[i]!.start + 1,
      stream,
    })),
  }
}

/**
 * Ranged path — server declared Accept-Ranges: bytes.
 * Fires condensed range GETs concurrently and distributes streams back to each
 * original range. Any non-206 response is a hard error.
 */
async function handleRanged<T extends ByteRange>(
  url: string,
  ranges: T[],
  totalSize: number,
  opts: ResolvedOptions,
): Promise<FetchByteRangesResult<T>> {
  const condensed = condenseRanges(ranges, opts.maxRequests)
  const fetchedMap = new Map<string, FetchedRange>()

  for (let i = 0; i < condensed.length; i += opts.concurrency) {
    const batch = condensed.slice(i, i + opts.concurrency)
    const fetched = await Promise.all(batch.map((range) => fetchRange(url, range, opts)))

    for (const result of fetched) fetchedMap.set(rangeKey(result.range), result)
  }

  const plan = buildPlan(ranges, condensed)
  const streams = distributeStream(null, ranges, plan, fetchedMap)

  return {
    totalSize,
    ranges: ranges.map((range, i) => ({
      range,
      contentRange: fetchedMap.get(rangeKey(plan[i]!.condensedRange))!.contentRange,
      byteLength: range.end - range.start + 1,
      stream: streams[i]!,
    })),
  }
}

interface ResolvedOptions {
  concurrency: number
  maxRequests: number
  headers: HeadersInit
  signal: AbortSignal | undefined
  fetch: typeof fetch
}

interface FetchedRange {
  range: ByteRange
  contentRange: string | null
  stream: ReadableStream<Uint8Array>
}

interface RangePlanEntry {
  condensedRange: ByteRange
  offsetInCondensed: number
}

interface PreflightResult {
  /** Size reported in Content-length by server. */
  totalSize: number

  /** True only when the server explicitly declared `Accept-Ranges: bytes`. */
  rangesSupported: boolean
}

export const resolveOptions = (options: FetchByteRangesOptions): ResolvedOptions => ({
  concurrency: options.concurrency ?? 6,
  maxRequests: options.maxRequests ?? 8,
  headers: options.headers ?? {},
  signal: options.signal,
  fetch: options.fetch ?? globalThis.fetch,
})

/**
 * HEAD preflight: establishes file size and range support.
 * Throws `MissingContentLengthError` if Content-Length is absent.
 */
export async function preflight(url: string, opts: ResolvedOptions): Promise<PreflightResult> {
  const response = await opts.fetch(url, {
    method: 'HEAD',
    headers: opts.headers,
    signal: opts.signal,
  })

  if (!response.ok) throw new Error(`Preflight HEAD ${url} returned ${response.status}`)

  const contentLength = response.headers.get('Content-Length')
  if (contentLength === null) throw new MissingContentLengthError(url)

  return {
    totalSize: parseInt(contentLength, 10),
    rangesSupported: response.headers.get('Accept-Ranges') === 'bytes',
  }
}

function buildPlan(originals: ByteRange[], condensed: ByteRange[]): RangePlanEntry[] {
  return originals.map((original) => {
    const condensedRange = condensed.find(
      ({ start, end }) => start <= original.start && end >= original.end,
    )

    if (!condensedRange)
      throw new Error(
        `No condensed range covers [${original.start}, ${original.end}] — this is a bug`,
      )

    return {
      condensedRange,
      offsetInCondensed: original.start - condensedRange.start,
    }
  })
}

/**
 * Distribute a source stream (or per-condensed-range streams) into one sliced
 * ReadableStream per original range.
 *
 * When `singleSource` is provided (full-body path), all ranges read from it.
 * When `singleSource` is null (ranged path), each group reads from its own
 * entry in `fetchedMap`.
 *
 * Multiple original ranges sharing a source are handled by tee-ing into a chain:
 *   source → [branchA, rest₁] → [branchB, rest₂] → … → restₙ₋₁
 */
function distributeStream(
  singleSource: ReadableStream<Uint8Array> | null,
  originals: ByteRange[],
  plan: RangePlanEntry[],
  fetchedMap?: Map<string, FetchedRange>,
): ReadableStream<Uint8Array>[] {
  const groupsByKey = new Map<string, number[]>()
  for (let i = 0; i < originals.length; i++) {
    const key = singleSource !== null ? '__full__' : rangeKey(plan[i]!.condensedRange)
    const group = groupsByKey.get(key)

    group ? group.push(i) : groupsByKey.set(key, [i])
  }

  const streamForOriginal = new Map<number, ReadableStream<Uint8Array>>()

  for (const [key, indices] of groupsByKey) {
    const source = singleSource ?? fetchedMap!.get(key)!.stream
    const condensedRange = plan[indices[0]!]!.condensedRange
    const condensedLength = condensedRange.end - condensedRange.start + 1

    if (indices.length === 1) {
      const origRange = originals[indices[0]!]!
      const byteLength = origRange.end - origRange.start + 1
      const passThrough =
        byteLength === condensedLength && plan[indices[0]!]!.offsetInCondensed === 0

      streamForOriginal.set(
        indices[0]!,
        passThrough
          ? source
          : sliceStream(source, plan[indices[0]!]!.offsetInCondensed, byteLength),
      )
    } else {
      let remaining = source

      for (let j = 0; j < indices.length; j++) {
        const origIndex = indices[j]!
        const byteLength = originals[origIndex]!.end - originals[origIndex]!.start + 1
        const isLast = j === indices.length - 1

        if (isLast) {
          streamForOriginal.set(
            origIndex,
            sliceStream(remaining, plan[origIndex]!.offsetInCondensed, byteLength),
          )
        } else {
          const [branch, next] = remaining.tee()
          streamForOriginal.set(
            origIndex,
            sliceStream(branch, plan[origIndex]!.offsetInCondensed, byteLength),
          )
          remaining = next
        }
      }
    }
  }

  return originals.map((_, i) => streamForOriginal.get(i)!)
}

async function fetchRange(
  url: string,
  range: ByteRange,
  opts: ResolvedOptions,
): Promise<FetchedRange> {
  const headers = new Headers(opts.headers)
  headers.set('Range', `bytes=${formatRange(range)}`)
  const response = await opts.fetch(url, { headers, signal: opts.signal })

  if (response.status === 416) throw new RangeNotSatisfiableError(range, response.status)

  if (response.status !== 206) throw new UnexpectedRangeResponseError(range, response.status)

  return {
    range,
    contentRange: response.headers.get('Content-Range'),
    stream: response.body ?? emptyStream(),
  }
}

/**
 * Wraps a source stream, forwards exactly `minBytes` bytes, then closes.
 * Errors the stream with `FullBodyTooShortError` if the source closes early.
 */
function guardStream(
  source: ReadableStream<Uint8Array>,
  minBytes: number,
): ReadableStream<Uint8Array> {
  const reader = source.getReader()
  let received = 0

  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (received >= minBytes) {
        controller.close()
        reader.cancel()
        return
      }

      const { done, value } = await reader.read()

      if (done) {
        controller.error(new FullBodyTooShortError(received, minBytes))
        return
      }

      const canEmit = minBytes - received
      const chunk = value.byteLength > canEmit ? value.subarray(0, canEmit) : value

      received += chunk.byteLength
      controller.enqueue(chunk)
    },
    cancel(reason) {
      reader.cancel(reason)
    },
  })
}

/**
 * Returns a ReadableStream that emits only bytes [offset, offset + length)
 * from the source, discarding bytes outside that window without buffering.
 */
function sliceStream(
  source: ReadableStream<Uint8Array>,
  offset: number,
  length: number,
): ReadableStream<Uint8Array> {
  const reader = source.getReader()
  let bytesSkipped = 0
  let bytesEmitted = 0

  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      while (bytesEmitted < length) {
        const { done, value } = await reader.read()
        if (done) {
          controller.close()
          return
        }

        let chunk = value

        if (bytesSkipped < offset) {
          const toSkip = offset - bytesSkipped

          if (chunk.byteLength <= toSkip) {
            bytesSkipped += chunk.byteLength
            continue
          }

          chunk = chunk.subarray(toSkip)
          bytesSkipped += toSkip
        }

        const canEmit = length - bytesEmitted
        if (chunk.byteLength > canEmit) chunk = chunk.subarray(0, canEmit)

        controller.enqueue(chunk)
        bytesEmitted += chunk.byteLength
      }

      controller.close()
      reader.releaseLock()
    },
    cancel(reason) {
      reader.cancel(reason)
    },
  })
}

function assertValidRanges(ranges: ByteRange[]): void {
  for (const { start, end } of ranges) {
    if (!Number.isInteger(start) || start < 0 || !Number.isInteger(end) || end < 0 || start > end) {
      throw new RangeError(
        `Invalid range [${start}, ${end}]: must be non-negative integers with start <= end`,
      )
    }
  }
}

function assertRangesInBounds(ranges: ByteRange[], totalSize: number): void {
  for (const range of ranges) {
    if (range.end >= totalSize) throw new RangeOutOfBoundsError(range, totalSize)
  }
}

const emptyStream = (): ReadableStream<Uint8Array> =>
  new ReadableStream({
    start(c) {
      c.close()
    },
  })

const formatRange = ({ start, end }: ByteRange): string => `${start}-${end}`

const rangeKey = ({ start, end }: ByteRange): string => `${start}:${end}`
