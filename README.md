# Stream library for go

Shpanstream is lightweight "zero-dependencies" Go library for working with streams in a simple and efficient way inspired by modern functional programming languages.

## At a glance

```go  
    // Find the first even number in a stream
    shpanstream.Just(1, 2, 3, 4, 5}).
        Filter(func(x int) bool {
            return x%2 == 0
        }).
        FindFirst().
        MustGet()
	
```

## But... go has channels

Channels are a low-level concurrency primitive, and is use by this library as the backbone of some of the stream implementations.
Working with low level channels requires a lot of boilerplate code and lack of composability and other tooling like filtering mapping or reducing.

ShpanStream Streams are a high-level abstraction for working with sequences of data. Streams provide a more functional approach to processing data, 
allowing you to easily compose operations and work with infinite sequences.

Streams are lazy, meaning that they only evaluate the data when needed. This allows for efficient processing of large data sets 
without loading everything into memory at once.

Streams allow memory efficient processing of large data sets. e.g. stream data from the database, manipulate it and stream json response to a client.

## 🚀 Install

```sh
go get github.com/shpandrak/shpanstream@v0.1.0
```

## Examples

### Functional and Composability
Streams can be easily composed together, allow building complex data processing pipelines.
using declarative functional "programming style" API, code can focus on higher-order functions to manipulate data.
While go generics limits generic functions, making the api less "fluent" than other languages, we can still achiever relatively clean flow

```go  
    
    // Prints the first 5 weapons of non Hobit LOTR characters
    shpanstream.MapStream(
        lotrCharactersRepo.Stream().
            Filter(func(c Character) bool {
                return c.Race != "Hobbit"
            }),
		func (c Character) string {
			return c.Weapon
	    }).
	    Limit(5).
        MustConsume(func (w string) {
            fmt.Println(w)
        })
	
```

### Concurrency
Concurrent processing is made easy. Stream operations can be evaluated concurrently by just adding "concurrent" option

See [Full flags example](examples/flags/flags_example.go)

Lets take this example, here is a non-concurrent pipeline to process a stream of country codes and maps it to a struct that contains the country name and flag SVG
```go

    // Synchronous processing of the stream
    shpanstream.MapStream(
        // Query external resource for all country codes
        fetchCountryCodes(),
        // For each country code, fetch the country flag via external resource
        fetchCountryFlag
    ).MustConsume(func (countryInfo CountryInfo) {,
        fmt.Println(countryInfo)
    })

    
```
Since for each country code we need to fetch the country flag from an external resource, this can take a while,
and we can easily make it faster by adding the "concurrent" option to the stream without
changing any of the logic of the piepline or introducing lowe level code to synchronize goroutines and collect the result.

```go

    // Concurrent processing of the stream 
    shpanstream.MapStream(
        // Query external resource for all country codes
        fetchCountryCodes(),
        // For each country code, fetch the country flag via external resource
        fetchCountryFlag,
        shpanstream.WithConcurrent(10), /* This is the only change */
    ).MustConsume(func (countryInfo CountryInfo) {
        fmt.Println(countryInfo)
    })
```

As you can test for yourself, using the [flags example](examples/flags/flags_example.go), this can make a huge difference in performance.
```
// Example outputs:
//
// concurrency of 10: "Flags example with concurrency of 10 finished in 421.168417ms"
// concurrency of 5:  "Flags example with concurrency of 5 finished in 874.123833ms"
// concurrency of 2:  "Flags example with concurrency of 2 finished in 1.737893375s"
// concurrency of 1:  "Flags example with concurrency of 1 finished in 3.412212583s"
```

> ⚠️ **Important**
> 
> When using the concurrent option, the order of the elements in the stream is not guaranteed to be
> preserved and is likely to be different from the original order.
> In addition, while the first example only loaded into memory one element at a time, the concurrent
> option will load more elements (up to the concurrency requested), take that into account when using


### Error propagation
Streams provide a clean way to handle errors, allowing you to easily propagate errors through your data processing pipeline.
Since streams are lazily evaluated, errors are only propagated when the stream is being materialized,
keeping the error handling to where it is relevant
functions that return Stream[T] doesn't have to return an error since when the stream is materialized it will be available
this allows streams operations to be composed together without having to add boilerplate 'if err != nil' where it is not needed

When there are errors, streams automatically close all the underlying resources (e.g database cursor, file...) and propagate the error to the final consumer

Let's go back to the country flags example, and add error handling to the pipeline
For simplicity, previous iteration ignored errors and used the `MapStream` that is meant for simple mapping
since fetching external resources can fail, we will use the `MapStreamWithErr` that allows the mapper to return an error


```go
    func fetchCountryFlagCanErr(string) (CountryInfo, error) {
        // Fetch the country flag from an external resource
        // This function can return an error if the request fails
        // ...
    }
	
	// if there is an error at any point in the pipeline, it will propagate to the final error and close the stream
	// with all the underlying resources cleaned up
    err: = shpanstream.MapStreamWithErr(
        // Query external resource for all country codes
        fetchCountryCodes(),
        // For each country code, fetch the country flag via external resource
        fetchCountryFlagCanErr, // this function signature allows returning an error
		shpanstream.WithConcurrent(10),
    ).Consume(context.Background(), func (countryInfo CountryInfo) {
        fmt.Println(countryInfo)
    })

    if err != nil {
        // do the "normal" go error handling :)
    }

```

What about when fetchCountryCodes()  fail?

Great question 🙂! Since fetchCountryCodesToNames() also access external resources, it might fail. 
Since it returns a stream, it the error is already 'included' , and will be propagated to the final consumer.

stream processing will fail if 
- stream init sequence fails (e.g. opening a file, an http request, a database connection, etc.)
- one of the underlying pipeline fails. (e.g. mapper function returns an error)

In cases function can't event create the stream (e.g. due to an invalid input) it can simply return an "Error stream".
when the stream materializes, the error will be propagated immediately to the final consumer 
and none of the downstream operations will be executed.

```go
func fetchCountryCodes() shpanstream.Stream[string] {

    if (currentUserDoesNotHavePermission()) {
        return shpanstream.ErrorStream[string](fmt.Errorf("user %s does not have permission", getCurrentUser()))
    }
    return shpanstream.Just("US", "CA", "GB")
}
```

### Context propagation
When using external resources, it is important to be able to cancel the request if the stream is cancelled.
for that there is an additional version of the MapStream function that in addition to supporting errors, allows passing context to the maper
While this is not needed in simple stream processing examples, it can be crucial when using external resources.
Passing context allows to cancel the request if the stream is cancelled, and also allows to set a timeout for the entire pipeline

```go
    func fetchCountryFlagFull(ctx context.Context, string) (CountryInfo, error) {
        // Fetch the country flag from an external resource
        // Pass the "materialization context" to the downstream requests so it can be cancelled
    }

	// Creating a context with a timeout for the entire pipeline execution
    ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	
    err: = shpanstream.MapStreamWithErrAndCtx(
        // Query external resource for all country codes
        fetchCountryCodes(),
        // For each country code, fetch the country flag via external resource
        fetchCountryFlagCanErr, // this function signature allows returning an error
		shpanstream.WithConcurrent(10),
    ).Consume(ctx, func (countryInfo CountryInfo) {
        fmt.Println(countryInfo)
    })


```

### Buffering
When using external resources, it is important to be able to buffer the data in order to avoid blocking the stream.
this will prevent"bursty" readers from blocking the stream and allow to process the data in a more efficient way.
```go
    // Buffer the stream of country codes before processing it
    stocks.StreamUpdates()
        Buffer(10).
        MustConsume(func (stockInfo StockInfo) {
            teller.Buy(stockInfo, 13)
        })
```

### Merging
There are multiple ways to merge streams together, a very useful one is to merge multiple streams that are already sorted.
This can be very handy with time series data that tends to be sorted by time, and have multiple sources of data.
```go
    // Merge two sorted streams of integers
    mergedStream := MergedSortedStream(
        cmp.Compare,
        Just(1, 4, 7),
        Just(2, 5, 8, 9),
        EmptyStream[int](),
        Just(3, 6, 9),
    )
    
    // Expected result after merging 
    expected := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 9}
    

```

### Paging 
When using underlying data sources that doesn't natively support paging streams can be easily paged.
Since streams are composable this also work when composing multiple streams together into a single pageable stream.

```go
    
    // Return page 2 of size 10 from the merged stream of LOTR and The Hobbit characters
    shpanstream.ConcatStreams(
        lotrCharactersRepo.Stream(),
        theHobbitCharactersRepo.Stream(),
    ).
    Page(2, 10).
        MustConsume(func (name string) {
            fmt.Println(name)
        })
```

### Json streaming tools
Since json is the de-facto standard for data interchange, and is used by most APIs, shpanstream provide some built-in stream providers functions to work with json data streams.
- ReadJsonArray: Read a json array from a reader and return a stream of the elements in the array
- ReadJsonObject: Read a json object from a reader and return a stream of the key-value pairs in the object
- StreamJsonToWriter/StreamJsonToWriterWithInit: Stream a stream of data to a writer as json

see example in the [Full flags example](examples/flags/flags_example.go) for a complete example of how to use these functions

### Time series stream processing example

Streams are very useful for processing time series data, allowing you to easily manipulate and analyze time series data
while we stream the data from the source. a common use case is to align time series data.
The aligner uses the ClusterSortedStream to align the time series data from a single source
See [Time series aligner example](examples/timeseries/timeseries_stream_aligner.go)

### Grpc streaming example

- [ ] TODO: Add Grpc streaming example


### Infinite streams
Streams can be infinite, allowing you to work with data that is generated on the fly.

- [ ] TODO: Add Infinite streams example

### Advance stream processing
Enabler for advanced data processing: Streams provide a powerful abstraction for working with data, allowing you to easily implement advanced data processing techniques such as map-reduce, windowing, bucketing and more.

- [ ] TODO: Add advanced stream processing example

## Documentation
[GoDoc](https://pkg.go.dev/github.com/shpandrak/shpanstream) 

## 📝 License

Copyright © 2025 [Shpandrak](https://github.com/shpandrak).

This project is under [MIT](./LICENSE) license.