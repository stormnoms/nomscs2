
See this Readme for more details:

https://github.com/stormasm/hackernoms2/blob/bp3/README.md

See this Readme for more details on Noms

https://github.com/attic-labs/noms/blob/master/README.md

This repo uses this commit point in Noms

https://github.com/attic-labs/noms/commit/9998ec030175f8650f83dd8d0715b42c83fda471

File changes to this point have been completed...

```
go/chunks/bolt_store.go
go/chunks/redis_store.go

go/spec/spec_store.go
go/spec/spec.go
go/spec/ref_counting_bolt_store.go
go/spec/ref_counting_redis_store.go
```

I have not pulled in any of the tests yet.

Theoretically the only file changes really needed if I got rid of the
ref_counting files and integrated that code into the respective stores
and put the spec_store code in spec would be 3 file changes namely...

```
go/chunks/bolt_store.go
go/chunks/redis_store.go

go/spec/spec.go
```
