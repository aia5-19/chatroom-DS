package main

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"chatroom/commons"
)

type user struct {
	inbox    []string  // messages waiting for this user to poll them
	lastSeen time.Time // last time this user talked to the server
}

type Chat struct {
	mu    sync.Mutex // protects users (RPC calls run in different goroutines)
	users map[string]*user
}

// ---- helpers (the caller must hold c.mu) ----

func (c *Chat) broadcast(text, except string) {
	for name, u := range c.users {
		if name != except {
			u.inbox = append(u.inbox, text)
		}
	}
}

func (c *Chat) names() []string {
	var list []string
	for name := range c.users {
		list = append(list, name)
	}
	return list
}

func (c *Chat) remove(name, why string) {
	delete(c.users, name)
	c.broadcast("*** "+name+" "+why, name)
}

// ---- RPC methods ----

func (c *Chat) Join(name string, users *[]string) error {
	name = strings.TrimSpace(name)
	c.mu.Lock()
	defer c.mu.Unlock()
	if name == "" {
		return errors.New("username cannot be empty")
	}
	if _, taken := c.users[name]; taken {
		return fmt.Errorf("username %q is already taken", name)
	}
	c.broadcast("*** "+name+" joined the chat", name)
	c.users[name] = &user{lastSeen: time.Now()}
	*users = c.names()
	fmt.Println(name, "joined")
	return nil
}

func (c *Chat) Leave(name string, ack *bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.users[name]; ok {
		c.remove(name, "left the chat")
		fmt.Println(name, "left")
	}
	return nil
}

func (c *Chat) Send(m commons.Msg, ack *bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	u, ok := c.users[m.Name]
	if !ok {
		return errors.New("you are not joined")
	}
	u.lastSeen = time.Now()
	c.broadcast(m.Name+": "+m.Text, m.Name) // everyone except the sender
	fmt.Printf("%s: %s\n", m.Name, m.Text)
	return nil
}

// Poll returns (and clears) the messages waiting for this user.
func (c *Chat) Poll(name string, msgs *[]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	u, ok := c.users[name]
	if !ok {
		return errors.New("you are not joined")
	}
	u.lastSeen = time.Now()
	*msgs = u.inbox
	u.inbox = nil
	return nil
}

func (c *Chat) Users(name string, users *[]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	*users = c.names()
	return nil
}

func main() {
	chat := &Chat{users: map[string]*user{}}
	rpc.Register(chat)

	// Port 0 = the OS picks any free port for us (auto-assigned).
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	fmt.Println("Server listening on port", ln.Addr().(*net.TCPAddr).Port)
	go rpc.Accept(ln)

	// A client that crashed stops polling: remove it after 5 seconds of silence.
	go func() {
		for range time.Tick(time.Second) {
			chat.mu.Lock()
			for name, u := range chat.users {
				if time.Since(u.lastSeen) > 5*time.Second {
					chat.remove(name, "left the chat (connection lost)")
					fmt.Println(name, "disconnected")
				}
			}
			chat.mu.Unlock()
		}
	}()

	// Ctrl+C: stop the server (clients notice and exit).
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	fmt.Println("Server shutting down")
}
