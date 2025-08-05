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
		Bucket: odin.Bucket{ID: "KeyValue"},
		Name:   "Andrew",
		Email:  "andrew@example.com",
	}

	if err := odin.Create(user); err != nil {
		log.Fatal(err)
	}
}
