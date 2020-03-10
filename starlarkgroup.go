// Copyright 2020 Edward McFarlane. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package starlarkgroup

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.starlark.net/starlark"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// Make creates a new group instance. Accepts the following optional kwargs:
// "n", "every", "burst".
//
// An application can add 'group' to the Starlark envrionment like so:
//
// 	globals := starlark.StringDict{
// 		"group": starlark.NewBuiltin("group", starlarkgroup.Make),
// 	}
//
func Make(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var n int
	var every string
	var burst int
	if err := starlark.UnpackArgs(
		"group", args, kwargs,
		"n?", &n, "every?", &every, "burst?", &burst,
	); err != nil {
		return nil, err
	}

	r := rate.Inf
	if every != "" {
		d, err := time.ParseDuration(every)
		if err != nil {
			return nil, err
		}
		r = rate.Every(d)
	}

	ctx, ok := thread.Local("context").(context.Context)
	if !ok {
		ctx = context.Background()
	}

	return NewGroup(ctx, n, r, burst), nil
}

type result struct {
	i int
	v starlark.Value
}

type byPos []result

func (r byPos) Len() int           { return len(r) }
func (r byPos) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r byPos) Less(i, j int) bool { return r[i].i < r[j].i }

// Group implements errgroup.Group in starlark with additional rate limiting.
type Group struct {
	ctx     context.Context
	group   *errgroup.Group
	limiter *rate.Limiter

	frozen bool

	n, i    int
	queue   chan func() error
	mu      sync.Mutex
	results []result
}

func (g *Group) String() string       { return "group()" }
func (g *Group) Type() string         { return "group" }
func (g *Group) Freeze()              { g.frozen = true }
func (g *Group) Truth() starlark.Bool { return starlark.Bool(!g.frozen) }
func (g *Group) Hash() (uint32, error) {
	return 0, fmt.Errorf("unhashable type: group")
}

var groupMethods = map[string]*starlark.Builtin{
	"go":   starlark.NewBuiltin("group.go", group_go),
	"wait": starlark.NewBuiltin("group.wait", group_wait),
}

func (g *Group) Attr(name string) (starlark.Value, error) {
	b := groupMethods[name]
	if b == nil {
		return nil, nil
	}
	return b.BindReceiver(g), nil
}

func (g *Group) AttrNames() []string {
	names := make([]string, 0, len(groupMethods))
	for name := range groupMethods {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func NewGroup(ctx context.Context, n int, r rate.Limit, b int) *Group {
	group, ctx := errgroup.WithContext(ctx)
	limiter := rate.NewLimiter(r, b)

	return &Group{
		ctx:     ctx,
		group:   group,
		limiter: limiter,
	}
}

func group_go(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("group.go: missing function arg")
	}
	fn, ok := args[0].(starlark.Callable)
	if !ok {
		return nil, fmt.Errorf("group.go: expected callable got %T", args[0])
	}

	g := b.Receiver().(*Group)
	if g.frozen {
		return nil, fmt.Errorf("group: frozen")
	}

	args.Freeze()
	for _, kwarg := range kwargs {
		kwarg.Freeze()
	}

	if g.ctx.Err() != nil {
		return starlark.None, nil // Context cancelled
	}

	args = args[1:]
	i := g.i
	g.i++

	g.limiter.Wait(g.ctx)

	call := func() error {
		thread := new(starlark.Thread)
		thread.SetLocal("context", g.ctx)

		v, err := starlark.Call(thread, fn, args, kwargs)
		if err != nil {
			return err
		}

		g.mu.Lock()
		g.results = append(g.results, result{i: i, v: v})
		g.mu.Unlock()
		return nil
	}

	if g.n <= 0 {
		g.group.Go(call)
		return starlark.None, nil
	}

	if i == 0 {
		g.queue = make(chan func() error, g.n)
	}

	if i < g.n {
		g.group.Go(func() error {
			for call := range g.queue {
				if err := call(); err != nil {
					return err
				}
			}
			return nil
		})
	}

	select {
	case g.queue <- call:
	case <-g.ctx.Done():
	}
	return starlark.None, nil
}

func group_wait(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	g := b.Receiver().(*Group)

	if g.frozen {
		return nil, fmt.Errorf("group.wait: frozen")
	}
	g.Freeze()

	if g.queue != nil {
		close(g.queue)
	}

	if err := g.group.Wait(); err != nil {
		return nil, err
	}

	sort.Sort(byPos(g.results))
	elems := make([]starlark.Value, len(g.results))
	for i, result := range g.results {
		elems[i] = result.v
	}

	list := starlark.NewList(elems)
	return list, nil
}
