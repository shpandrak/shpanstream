# Integration with oapi-codegen

this template allows automatic generation streaming responses as part of the oapi-codegen tool.

## Setup
to use it, add the following to the oapi-config.yaml file:

```yaml
additional-imports:
  - alias: stream
    package: github.com/shpandrak/shpanstream/stream
  - alias: jsonstream
    package: github.com/shpandrak/shpanstream/utils/jsonstream
output-options:
  user-templates:
    strict/strict-interface.tmpl: https://raw.githubusercontent.com/shpandrak/shpanstream/master/integrations/oapi-codegen/templates/strict-interface.tmpl
```
Pin the URL to a tag or commit instead of `master` for reproducible generation. The template
version must match the `github.com/shpandrak/shpanstream` version in `go.mod`.

## Usage
The result is generating more response type that support streaming responses.
example usage:

```go

func (o OapiHubImpl) ListEggs(
	ctx context.Context,
	request openapi.ListEggsRequestObject,
) (openapi.ListEggsResponseObject, error) {

	return openapi.StreamingListEggs200JSONResponseObject{
		Ctx: ctx,
		Stream: stream.Map(
			o.eggsService.ListEggs(eggs.VendorId(request.VendorId)),
			o.mapEggDomainToApi,
		),
	}, nil
}

```

The response will not materialize the stream and stream one by one item to the client as they are produced. and the client reads.

## Errors mid-stream

`StreamJsonToHttpResponseWriter` holds back the `200` and the body until 32 KiB
(`jsonstream.DefaultCommitThreshold`) have been produced or the stream ends. A stream that fails
before that returns its error with **nothing written**, so your response error handler can set
whatever status it wants. Responses under 32 KiB are therefore all-or-nothing.

A stream that fails after that point cannot be repaired: the client already has a `200` and a
truncated JSON array. The function expires the response's write deadline before returning, so the
connection is torn down (HTTP/1.1 closes before the terminating chunk, HTTP/2 sends `RST_STREAM`)
and the client sees a read error rather than a well-formed-looking response. Anything your error
handler writes afterwards, including oapi-codegen's default `http.Error`, never reaches the
client. The error you get back is a `*jsonstream.ResponseCommittedError`; match it to log the
cause instead of trying to send a status:

```go
func handleResponseError(w http.ResponseWriter, r *http.Request, err error) {
	var committed *jsonstream.ResponseCommittedError
	if errors.As(err, &committed) {
		log.Printf("stream failed after commit: %v", committed.Unwrap())
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}
```

The teardown needs `http.ResponseController` to reach the real writer. A middleware that wraps
`w` without an `Unwrap()` method defeats it; there, `panic(http.ErrAbortHandler)` from the error
handler is the fallback.

Streams that trickle slowly and need the first byte out early can opt out of buffering per
endpoint through the `Options` field of the generated response object:

```go
return openapi.StreamingListEggs200JSONResponseObject{
	Ctx:     ctx,
	Stream:  eggs,
	Options: []jsonstream.HttpStreamOption{jsonstream.WithCommitThreshold(0)},
}, nil
```

or, when calling the function directly, `jsonstream.StreamJsonToHttpResponseWriter(ctx, w, s,
jsonstream.WithCommitThreshold(0))`.

Hand-written response bodies get the same all-or-nothing behaviour from
`httputil.BufferedResponseWriter`: wrap `w`, write as usual, call `Commit()` at the end. On an
error, check `Committed()`; if false, write the error response to `Unwrap()`; if true, tear the
connection down, for example `http.NewResponseController(w).SetWriteDeadline(time.Now())`, and log.
