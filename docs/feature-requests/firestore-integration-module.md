# Feature request: an `integrations/firestore` module

*Filed by a consumer in production, pinned at `v0.4.57`, that runs on Cloud Firestore. Checked
against master `bc9c340` on 2026-09-18.*

## Ask

Add a separate Go module, `github.com/shpandrak/shpanstream/integrations/firestore`, laid out like
`integrations/sql`: its own `go.mod`, and `replace => ../../` during development. It would hold
three small adapters that a consumer wrote for itself and that any Firestore user of shpanstream
needs. The root module stays dependency-free.

## What moves, and why it's generic

**1. Query to stream** (55 LOC in the consumer).
- It is a `stream.Provider` over a `firestore.Query`.
- `Open` creates the `DocumentIterator` using the context the stream is consumed with.
- `Emit` calls `Next()` and maps `iterator.Done` to `io.EOF`.
- `Close` calls `Stop()`.
- It is lazy and reads page by page: the client's gRPC paging drives it, and nothing is collected.

**2. Document refs to stream** (43 LOC).
- The same pattern over `CollectionRef.DocumentRefs`, which lists document ids, including ids of
  missing documents that only have subcollections under them.

**3. Awaited bulk write** (46 LOC), which fixes a silent failure.

The `BulkWriter` API is easy to misread in a way that loses errors:
- the error `bulk.Set/Delete/Create` returns covers **enqueueing** only;
- `Flush()` returns nothing at all;
- a server rejection is visible only through each `*BulkWriterJob`'s `Results()`.

So code that enqueues, flushes and returns `nil` reports success for writes the server refused. The
consumer found this on a one-shot migration: its ledger recorded "applied" over data that was
still there. A stream sink that awaits every job is the right shape for the library.

## Proposed API (a sketch, open to redesign)

```go
package firestore // github.com/shpandrak/shpanstream/integrations/firestore

// StreamQuery streams q's documents lazily: the iterator is created on Open with the
// materialization context, and stopped on Close.
func StreamQuery[T any](q firestore.Query, mapper func(*firestore.DocumentSnapshot) (T, error), opts ...Option) stream.Stream[T]

// StreamDocumentRefs streams the document references of a collection, including missing
// documents that only have subcollections under them.
func StreamDocumentRefs(col *firestore.CollectionRef, opts ...Option) stream.Stream[*firestore.DocumentRef]

// WithTransactionResolver is consulted on Open: when it returns a transaction, the read runs
// as tx.Documents / tx.DocumentRefs, so a stream consumed inside RunTransaction reads under it.
func WithTransactionResolver(resolve func(ctx context.Context) *firestore.Transaction) Option

// BulkWrite consumes s into one BulkWriter, flushes it, and returns the first real
// server-side failure (naming the document), not merely a queueing failure.
func BulkWrite[T any](ctx context.Context, client *firestore.Client, s stream.Stream[T],
    enqueue func(bw *firestore.BulkWriter, v T) (*firestore.BulkWriterJob, *firestore.DocumentRef, error)) error
```

The transaction resolver matters. The consumer carries its transaction in the context, and its
streams take no context of their own. Resolving the transaction on `Open` is what lets a query
consumed inside `RunTransaction` read under that transaction; a Firestore transaction requires all
its reads to be made through it. Where the transaction lives is up to each app, which is why this
is a hook.

## Acceptance tests

Run against the Firestore emulator, skipped when `FIRESTORE_EMULATOR_HOST` is unset:
1. `StreamQuery` over N documents yields N items. Collecting it twice yields them twice, because
   the query runs again on each `Open`.
2. With a resolver that returns a transaction, the read goes through the transaction. Consuming the
   stream *after* a write in the same transaction fails with the client's read-after-write error.
   That only happens if the query really went through `tx`. Without a resolver, the same sequence
   succeeds.
3. `BulkWrite`, with one write the server rejects (for example `Create` on an existing document),
   returns an error naming that document, even though enqueueing succeeded.
4. `Close` stops the iterator when the consumer quits early (`Limit`, `FindFirst`).

## What the consumer does once it lands

- Replace its two providers and its bulk-write helper with the module, passing its own context-key
  lookup as the resolver.
- Keep its document-format layer (native timestamps and integer handling) in the app, as the
  `mapper` / `enqueue` functions. That layer is specific to the app.
- Net effect: about 100 LOC fewer in the consumer.

## Workaround today

None needed. The code works where it is. This request is about sharing it, and about protecting
other consumers from the `BulkWriter` trap.

## Size and priority

- Size: small in code. The emulator test setup is most of the work.
- Priority: low for this consumer, which gains about 100 LOC. It is the only piece of the
  consumer's infrastructure that clearly fits shpanstream's scope. The rest of its storage layer, a
  JSON document store with an in-memory Firestore stand-in, is a store abstraction rather than a
  stream library, and stays in the app.
