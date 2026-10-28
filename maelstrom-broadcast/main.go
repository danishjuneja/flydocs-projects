package main

import (
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	// Register challenge handlers here with n.Handle(...).

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
