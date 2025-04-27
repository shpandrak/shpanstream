# Stream library for go

Shpanstream is lightweight "zero-dependencies" Go library for working with streams, inspired by modern functional programming languages.

## At a glance

```go  
// Find the first even number in a stream
Just(1, 2, 3, 4, 5).
    Filter(func(x int) bool {
        return x%2 == 0
    }).
    FindFirst().
    MustGet()
	
```

## But... go has channels

Channels are powerful low-level concurrency primitives and are the backbone of Go's concurrency model.
Shpanstream library is work on top of channels, among other options for steam source.

To get create a stream from a channel, you can use the `FromChannel` function:
```go
ch := make(chan string)

stream.FromChannel(ch).
    Filter(func(x string) bool {
        return len(strings) > 10
    }).
    //... the rest of the pipeline
```

While channels are great, they lack the composability needed for building complex data processing pipelines.
Shpanstream streams provide a higher-level abstraction for working with streams of data, 
allowing you to easily compose operations and work with infinite sequences.

shpanstream reduces the boilerplate for coordination and resource management needed to work directly with channels, 
while adding features as we'll see below.

ShpanStream offers memory efficient processing of large data sets. e.g. stream data from the database, 
manipulate it and stream json response to a clients.

## Addressing the elephant in the room

So... Go is not a functional programming language, not even close. Go syntax makes it impossible to write "fluent" functional code like in other languages.

E.g. the following typescript code:

```typescript

people
    .filter(p => p.age > 18)
    .map(p => p.name)
    .findFirst()

````

is not possible in Go. go generics as implemented today do not allow generics when invoking methods on a type.

with shpanstream you can write similar code, but it will be less fluent and more verbose.
we have to use the `MapStream` function to map the stream.

```go
MapStream(
    people.
        Filter(func(p Person) bool {
            return p.Age > 18
        }),
	func (p Person) string {
        return p.Name
    }).
    FindFirst().
    MustGet()
```

Together with go's verbose lambda syntax, and error handling model, it makes the code less fluent and more verbose than in other languages.

But... This alone doesn't mean we have to throw the baby out with the bathwater.
when working with stream processing, functional programming paradigm is more than just syntax sugar.

With shpanstream we can write code that is less imperative, moving a lot of the complex repetitive streaming code to the library, 
preventing hours of debugging and ensuring that the code is more maintainable. 
Specifically, when working with stream processing, encapsulating the resource management concern is a life saver.

With that out of the way, let's see what shpanstream has to offer.

Time to unleash the power of functional programming for gophers!

## 🚀 Install

```sh
go get github.com/shpandrak/shpanstream@v0.2.0
```

## Examples

### Functional and Composability
Streams can be easily composed together, allow building complex data processing pipelines.
using declarative functional "programming style" API, code can focus on higher-order functions to manipulate data.
While go generics limits generic functions, making the api less "fluent" than other languages, we can still achiever relatively clean flow

```go  
    
// Prints the first 5 weapons of non Hobit LOTR characters
stream.MapStream(
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
err: = stream.MapStreamWithErr(
    // Query external resource for all country codes
    fetchCountryCodes(),
    // For each country code, fetch the country flag via external resource
    fetchCountryFlagCanErr, // this function signature allows returning an error
    stream.WithConcurrent(10),
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
func fetchCountryCodes() stream.Stream[string] {

    if (currentUserDoesNotHavePermission()) {
        return stream.ErrorStream[string](fmt.Errorf("user %s does not have permission", getCurrentUser()))
    }
    return stream.Just("US", "CA", "GB")
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

err: = stream.MapStreamWithErrAndCtx(
    // Query external resource for all country codes
    fetchCountryCodes(),
    // For each country code, fetch the country flag via external resource
    fetchCountryFlagCanErr, // this function signature allows returning an error
    stream.WithConcurrent(10),
).Consume(ctx, func (countryInfo CountryInfo) {
    fmt.Println(countryInfo)
})
```

### Using channels
Using channels is a great way to get data into Shpanstream. Just use the `FromChannel` function to create a stream from a channel.
The channel can be either a buffered or unbuffered channel. from the stream perspective, it doesn't matter.
The stream will stay open until the channel is closed, and will automatically close and collapse the pipeline accordingly.
In case the channel is never closed, the stream will keep processing as any other infinite stream source

for full example using channels see the [Channel Test Example](channel_stream_provider_test.go)

Since channels are powerful, shpanstream uses them internally to implement some of the out-of-the-box functionality.
for example "Buffer" exposes a shpanstream backed by a buffer channel. see [Buffer implementation](buffered_stream.go)

```go

### Iterate over a stream
Shpanstream supports iterating over a stream using the range operator via the `Iterate` function.
The stream will be automatically closed when the iteration is done, and the pipeline will be collapsed accordingly.

```go
for curr := range Just(1, 1, 2, 3, 5, 8, 13, 21, 34, 55).Iterator {
    fmt.Sprintf(" %d", curr)
}
```

### Concurrency
While shpanstream focuses on memory efficient sequential processing of data, and allow others to build concurrent processing on top of it,
it does provide simple helpers to allow simple concurrent manipulation of streams.
Concurrent processing is made easy. Stream operations can be evaluated concurrently by just adding "concurrent" option

See [Full flags example](examples/flags/flags_example.go)

Lets take this example, here is a non-concurrent pipeline to process a stream of country codes and maps it to a struct that contains the country name and flag SVG
```go

// Synchronous processing of the stream
stream.MapStream(
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
stream.MapStream(
    // Query external resource for all country codes
    fetchCountryCodes(),
    // For each country code, fetch the country flag via external resource
    fetchCountryFlag,
    stream.WithConcurrent(10), /* This is the only change */
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

### Redcue

Shpanstream provides a standard reduce implementation for streams



```go
sum := MustReduce(
    Just(2, 4, 6),
    0,
    func(acc, v int) int {
        return acc + v
    },
)

// Output: 12
fmt.Println(sum)

```

### Sliding windows
Shpanstream provides a simple way to create sliding windows over streams of data, allowing you to easily manipulate and analyze data over time.

```go
// demonstrates how to use the Window function with a sliding window.
// It returns the first window of 3 elements that contains at least 2 even numbers.
results := Window(
    Just(1, 3, 3, 5, 11, 6, 7, 8, 8, 8, 10, 10, 12, 13, 14, 15, 16, 17, 18, 19),
    3,
    WithSlidingWindowStepOption(1),
).
    Filter(func(currWindow []int) bool {
        return Just(currWindow...).
            Filter(func(src int) bool {
                return src%2 == 0
            }).MustCount() >= 2
    }).FindFirst().MustGet()

// Print the result
fmt.Println(results)
// Output: [6 7 8]
```

### Buffering
When using external resources, it is important to be able to buffer the data in order to avoid blocking the stream.
this will prevent"bursty" readers from blocking the stream and allow to process the data in a more efficient way.
```go
// Buffer the stream of country codes before processing it
Buffer(stocks.StreamUpdates(), 10).
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
stream.ConcatStreams(
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
See [Time series aligner example](utils/timeseries/timeseries_stream_aligner.go)

### Websocket streaming example
Another common use case for streams is to stream data from a websocket connection.
websocket backed streams are often being uses as "infinite" streams that are processing the pipeline of data
as long as the connection is open.

In the [stocks example](integrations/ws/examples/stocks/stocks_example.go)
we are streaming stock prices from a websocket connection and processing the data in real time.
The stream is aligned mapped to timeseries data and aligned to produce consistent time series data

```go
// Align the stream to 3 seconds getting weighted average for price
timeseries.AlignStream(

    // Map stock entries prices to timeseries.TsRecord[float64] (while filtering irrelevant data)
    stream.MapStreamWhileFiltering(
        // Get the websocket stocks stream
        ws.CreateJsonStreamFromWebSocket[StockDto](createWebSocketFactory(apiKey)),

        // Map to timeseries.TsRecord[float64]
        mapStockToTimeSeries,
    ),
    3*time.Second,
).
    // Print the output to stdout
    Consume(ctx, func(t timeseries.TsRecord[float64]) {
        fmt.Printf("%+v\n", t)
    })

```

### Grpc streaming example

- [ ] TODO: Add Grpc streaming example


### Infinite streams
Streams can be infinite, allowing you to work with data that is generated on the fly.

- [ ] TODO: Add Infinite streams example

### Advance stream processing
Enabler for advanced data processing: Streams provide a powerful abstraction for working with data, allowing you to easily implement advanced data processing techniques such as map-reduce, windowing, bucketing and more.

- [ ] TODO: Add advanced stream processing example

## Repository structure

The philosophy of the repository structure is to keep the library minimal and zero-dependencies.
If user wants to use a specific integration, or a utility, they can simply import the additional package and use it.
In case the integration or utility requires additional dependencies, or is large, it will be declared as a separate go module.
the same for examples

Utilities are helper functions and/or types that use the core library to provide additional functionality. e.g. time series aligner, json streaming, etc.
Integrations are external integrations that implement usually implement a specific "provider" to allow integration with a specific data source.
E.g. websocket, grpc, kafka, postgres etc.

The repository is structured as follows:

```
├common library files
│ ....
├── stream
│   ├── all "Stream" types and functions
│   ├── ...
├── lazy
│   ├── all "Lazy" types and functions
│   ├── ...
├── examples
│   ├── example1
│   ├── ...
├── integrations
│   ├── integration1
│   ├── ...
├── utils
│   ├── util1
│   ├── ...
```

## Documentation
[GoDoc](https://pkg.go.dev/github.com/shpandrak/shpanstream) 

## 📝 License

Copyright © 2025 [Shpandrak](https://github.com/shpandrak).

This project is under [MIT](./LICENSE) license.