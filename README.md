# Odin | Database Wrapper for BBolt / BoltDB

Odin is a simple and efficient database wrapper for [BBolt](https://github.com/etcd-io/bbolt) (BoltDB), designed to make working with key-value stores in Go easy and intuitive.

## Features

- Struct-based data modeling
- Simple CRUD operations
- Automatic bucket management
- Easy connection handling

## Installation

```bash
go get github.com/andr1ww/odin
```

## Example

```go
package main

import (
    "log"

    "github.com/andr1ww/odin"
)

type User struct {
    odin.Bucket `bucket:"users" database:"main"`
    Name        string `json:"name"`
    Email       string `json:"email"`
}

func main() {
    if err := odin.ConnectDefault("./odin.db"); err != nil {
        log.Fatal(err)
    }
    defer odin.CloseAll()

    user := &User{
        Bucket: odin.Bucket{ID: "Key"},
        Name:   "Andrew",
        Email:  "andrew@example.com",
    }

    if err := odin.Create(user); err != nil {
        log.Fatal(err)
    }
}
```

**Disclaimer**: This was mainly a project for fun and research, Code is ass and looks AI im aware.
