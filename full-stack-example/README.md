# Full Stack Example

This is an example Note taking application that pulls together from this monorepo:
- composable endpoint schemas, based on Named Tuple based structural types, consumed by:
  - Browser fetch-api (Future) based client on frontend
  - com.sun.net.httpserver (direct style) server on backend
- JSON serialization/deserialization of named tuple values
- DataFrame processing of text statistics (with named tuple structural types)
- Database query DSL on backend (based on named tuple structural types)

The frontend is built in Scala.js as a single page app using Laminar.

The demo is based upon https://github.com/adpi2/scala3-full-stack-example
but adapted to use laminar and replace cask server.
