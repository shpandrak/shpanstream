# Feature request: a stream that fails midway must not become a `200` with a corrupted body

*Requested by shpankids, a consumer pinned at `v0.4.57` that uses the strict-interface oapi
template. Reproduced on master `bc9c340` on 2026-09-18.*

## Ask

Two changes to the HTTP JSON streaming path:
- `jsonstream.StreamJsonToHttpResponseWriter`
- the `Streaming…ResponseObject.Visit…` in `integrations/oapi-codegen/templates/strict-interface.tmpl`

The changes:
1. **Hold back the status until N bytes are buffered.** Buffer the start of the array, up to a
   configurable size such as 32 KiB, and send `200` only when that buffer fills or the stream ends.
   Then a stream that fails before N bytes returns its error with **nothing written**, and the
   error handler can still set the right status code.
2. **Make a failure after the status is sent recognisable.** Once the status has gone out, return
   an error the caller can detect, for example `*jsonstream.ResponseCommittedError` wrapping the
   cause and matched with `errors.As`. The error handler can then abort the connection
   (`panic(http.ErrAbortHandler)`) instead of writing more bytes into a `200`.

## Current behaviour (probe)

The probe streams `Just(1,2,3,4,5)` through a `MapWithErr` that fails at element N, then does what
the generated strict handler does with its error: `ResponseErrorHandlerFunc` → `http.Error`.

```
fail at element 1: status=500 content-type="text/plain; charset=utf-8" body="boom at element\n"
http: superfluous response.WriteHeader call ...
fail at element 3: status=200 content-type="application/json" body="[1,2boom at element\n"
```

The code responsible:
- `StreamJsonToHttpResponseWriter` sends `Content-Type` and `WriteHeader(200)` as soon as the first
  element arrives (`utils/jsonstream/http_json_streaming_util.go:11-15`).
- When the stream fails later, the generated handler passes the error to the consumer's error
  handler. The headers are already sent at that point, so whatever that handler writes is appended
  to the half-written JSON array.

The client receives a `200` whose body isn't valid JSON, and whose content includes the server's
error text. Status-based monitoring counts the request as a success.

## Why shpankids needs it

**The size of the problem.** shpankids serves 27 array endpoints through this template. Most of
them fail *before* the first element and already get the right status:
- 10 build a slice first and wrap it with `stream.Just`.
- 12 read everything on their first pull.

Failures after the first element are rare. They come from two places:
- a per-element mapping error, such as a stored document that won't decode, or the per-member user
  lookup inside `ListAllRealms`'s `MapWithErrAndCtx`;
- a Firestore iterator error in the middle of a long stream, such as the superuser `DumpData`.

**The consequences.** When one does happen:
- the kid's app sees a JSON parse failure instead of the server's error;
- Cloud Run's request metrics record a success;
- the only trace is a log line (`api.HandleErrors`'s `slog.Error`).

**How change 1 alone helps.** Buffering alone makes almost every shpankids response all-or-nothing,
because almost all of them are well under 32 KiB.

## Proposed behaviour

- **Buffer threshold.** `StreamJsonToHttpResponseWriterWithOptions(ctx, w, s,
  WithCommitThreshold(n))`, or a package-level default that the existing function uses. A
  threshold of `0` keeps today's behaviour.
- **Before the threshold:** on error, return it with nothing written. On a normal end, write the
  headers and `200`, then the buffered bytes.
- **After the threshold:** flush the buffer, stream directly, and wrap any later error as
  `ResponseCommittedError`.
- **The library never panics on its own.** Consumers often wrap handlers in a recovery middleware
  that would swallow `http.ErrAbortHandler`. shpankids' `panicRecoveryMiddleware` would, and would
  then append a JSON error body, which is the same corruption. Returning a recognisable error
  leaves the abort to the consumer, who knows their middleware stack.
- **Optional:** the template's `Visit…` could document the pattern, or offer an opt-in flag that
  aborts itself.

## Acceptance tests

1. A stream that fails before the threshold: nothing is written; the returned error is the
   original one, not a `ResponseCommittedError`; an error handler can set any status.
2. A stream that fails after the threshold: `errors.As(err, &jsonstream.ResponseCommittedError{})`
   is true, and nothing more is written after the error.
3. An empty stream and a small successful stream: byte-for-byte the same output as today, `[]` and
   `[...]`, with status `200` and `Content-Type: application/json`.
4. A large successful stream (bigger than the threshold): streamed, and the result is valid JSON.

## What shpankids does once it lands

1. Bump the pin: `go.mod` together with the template URL in `tools/oapi-conf.yaml`.
2. In `api.HandleErrors`, on `ResponseCommittedError`: log the error, then
   `panic(http.ErrAbortHandler)`.
3. Change `panicRecoveryMiddleware` to re-panic `http.ErrAbortHandler` instead of writing a body.

## Workaround today

This can be done without the library. shpankids could add a middleware that:
- wraps `http.ResponseWriter` to record whether `WriteHeader` or `Write` has run;
- makes `HandleErrors` abort instead of calling `http.Error` when it has.

That fixes change 2's symptom but not change 1: status codes stay wrong for failures after the
first element. Every consumer of the template would have to repeat it.

## Size and priority

- Size: small to medium. A buffering writer, an error type, and template notes.
- Priority: medium. It's a correctness issue for every consumer of the template, but it rarely
  triggers in shpankids.
