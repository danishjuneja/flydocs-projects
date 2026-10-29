package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	var count atomic.Uint64
	// Register challenge handlers here with n.Handle(...).
	n.Handle("add", func(msg maelstrom.Message) error {
		var body struct {
			Delta int `json:"delta"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		//
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		for {
			current, err := kv.ReadInt(ctx, "counter")
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				current = 0
			} else if err != nil {
				return err
			}

			err = kv.CompareAndSwap(ctx, "counter", current, current+body.Delta, true)

			if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
				// Another update won. Read again and retry.
				continue

			}
			if err!= nil{
				return err
			}
			return n.Reply(msg, map[string]any{
				"type": "add_ok",
			})
		}
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

		defer cancel()
		barrierValue := n.ID() + "-" + strconv.FormatUint(count.Add(1), 10)
		kv.Write(ctx, "read-barrier", barrierValue)
		value, err := kv.ReadInt(ctx, "counter")

		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist{
			value = 0
		}else if err!= nil{
			return err
		}
		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": value,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

