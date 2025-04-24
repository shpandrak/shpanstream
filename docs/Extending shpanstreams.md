# Extending shpanstreams

## Overview

Providers are the backbone of Shpanstream abstraction, responsible for managing getting the data retrieval from the source into the Shpanstream ecosystem.
Once the data is in the Shpanstream ecosystem, the stream is agnostic to the source of the data.

## Default providers (and channels)

For many external sources of data, There is no need to write a dedicated provider.
Thanks to go libraries being mostly idiomatic, for many external sources of data, the default providers built into the core library are sufficient.

Most notably, every library that exposes a channel interface can be used as a provider.
Channels are the most common way to get data into Shpanstream.

## Writing a provider
Todo:
## Writing a "Down stream" Provider
Todo:
