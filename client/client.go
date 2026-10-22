package main

import (
	"bufio"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"time"

	"chatroom/commons"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: go run ./client <server-address>   e.g. localhost:41234")
	}
	c, err := rpc.Dial("tcp", os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	in := bufio.NewScanner(os.Stdin)
//for join users
	var name string
	var users []string
	for {
		fmt.Print("Username: ")
		if !in.Scan() {
			return
		}
		name = strings.TrimSpace(in.Text())
		err = c.Call("Chat.Join", name, &users)
		if err == nil {
			break
		}
		if _, isServerErr := err.(rpc.ServerError); !isServerErr {
			log.Fatal(err) // network connection problem 
		}
		fmt.Println("Error:", err)
	}
	fmt.Println("Welcome! Online:", users, "| commands: /users /quit")

	var leaving atomic.Bool
	leave := func() {
		leaving.Store(true)
		var ack bool
		c.Call("Chat.Leave", name, &ack)
		fmt.Println("Bye!")
		os.Exit(0)
	}

	// Ctrl+C = leave cleanly.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() { <-sig; leave() }()

	// Polling
	go func() {
		for range time.Tick(500 * time.Millisecond) {
			var msgs []string
			if leaving.Load() {
				return
			}
			if err := c.Call("Chat.Poll", name, &msgs); err != nil && !leaving.Load() {
				fmt.Println("\nDisconnected from server:", err)
				os.Exit(0)
			}
			for _, m := range msgs {
				fmt.Println(m)
			}
		}
	}()

	// Read what the user types.
	for in.Scan() {
		switch line := strings.TrimSpace(in.Text()); line {
		case "":
		case "/quit":
			leave()
		case "/users":
			c.Call("Chat.Users", name, &users)
			fmt.Println("Online:", users)
		default:
			var ack bool
			if err := c.Call("Chat.Send", commons.Msg{Name: name, Text: line}, &ack); err != nil {
				fmt.Println("Error:", err)
			}
		}
	}
	leave() // Ctrl+D / end of input
}
