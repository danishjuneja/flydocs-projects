package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Entry struct {
	Offset  int
	Message int
}

func main() {
	var mu sync.Mutex

	logs := make(map[string][]Entry)

	//store till what offset entries have been committed
	committed := make(map[string]int)
	n := maelstrom.NewNode()

	// Register challenge handlers here with n.Handle(...).

	n.Handle("send", func(msg maelstrom.Message) error {
		var body struct {
			Key string `json:"key"`
			Msg int    `json:"msg"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		offset := len(logs[body.Key])
		logs[body.Key] = append(logs[body.Key], Entry{
			Offset:  offset,
			Message: body.Msg,
		})
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": offset,
		})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body struct {
			Offsets map[string]int `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		messages := make(map[string][][2]int)

		mu.Lock()
		for key, startOffset := range body.Offsets {
			for _, entry := range logs[key] {
				if entry.Offset >= startOffset {
					messages[key] = append(messages[key], [2]int{
						entry.Offset,
						entry.Message,
					})
				}
			}
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "poll_ok",
			"msgs": messages,
		})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body struct {
			Offsets map[string]int `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		for key, offset := range body.Offsets {
			committed[key] = offset
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "commit_offsets_ok",
		})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body struct {
			Keys []string `json:"keys"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offsets := make(map[string]int)
		mu.Lock()
		for _, key := range body.Keys{
			if offset, exists := committed[key]; exists{
				offsets[key] = offset
			}
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "list_committed_offsets_ok",
			"offsets": offsets,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

/*
Things I should be storing
key (string) | msg (string) | offset (int) | committed (bool)

since its an append only logging, we can store each new key in a seperate container
operations
send - store msg, add offset + 1, create if key doesn't exist
poll - go through each existing key and create array of [offset, message]
commit-offsets - go through each key and mark offsets less than or equal to given offset as true
list-committed-offsets - go through each key and find all committed offsets, return an array for each key


key is the main distinguishing factor so we store things by keys

key, struct
struct {msg, offset, committed}

add mutex locks whenever writing or reading to one key
*/
