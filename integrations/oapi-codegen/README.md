# Integration with oapi-codegen

this template allows automatic generation streaming responses as part of the oapi-codegen tool.

## Setup
to use it, add the following to the oapi-config.yaml file:

```yaml
additional-imports:
  - alias: stream
    package: github.com/shpandrak/shpanstream/stream
output-options:
  user-templates:
    https://raw.githubusercontent.com/shpandrak/shpanstream/v0.3.15/integrations/oapi-codegen/templates/strict-interface.tmpl
```
user a tag or a specific commit hash for fine grain version...

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