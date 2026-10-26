# Profile lookup

English | [中文](README.zh.md)

Query a simulated remote directory and receive one complete profile. No network or credentials are needed.

From the project root, with Go 1.27+:

```sh
go run ./examples/profile
```

```text
profile: alice (alice@example.com)
```

`Directory.Lookup` starts the lookup with `actorx.StartTask` and returns without blocking the actor. The completion callback runs serially on the actor, clears the loading state, and completes the deferred reply. `GetStatus` remains available while the lookup is running. Only one lookup is accepted at a time.

See [chat](../chat/README.md) for streaming and early cancellation. After changing handlers, run `go generate ./examples/profile`.
