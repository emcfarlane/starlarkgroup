// Copyright 2020 Edward McFarlane. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package starlarkgroup

import (
	"context"
	"fmt"
	"sort"
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

type callable struct {
	fn     starlark.Callable
	args   starlark.Tuple
	kwargs []starlark.Tuple
}

// Group implements errgroup.Group in starlark with additional rate limiting.
// Arguments to go call are frozen. Wait returns a sorted tuple in order of
// calling. Calls are lazy evaluated and only executed when waiting.
type Group struct {
	ctx     context.Context
	group   *errgroup.Group
	limiter *rate.Limiter

	frozen bool

	n     int
	calls []callable
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
	kwargs = make([]starlark.Tuple, len(kwargs))
	for i, kwarg := range kwargs {
		kwarg.Freeze()
		kwargs[i] = kwarg
	}

	if g.ctx.Err() != nil {
		return starlark.None, nil // Context cancelled
	}

	g.calls = append(g.calls, callable{
		fn:     fn,
		args:   args[1:],
		kwargs: kwargs,
	})
	return starlark.None, nil
}

func group_wait(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	g := b.Receiver().(*Group)
	if g.frozen {
		return nil, fmt.Errorf("group.wait: frozen")
	}
	g.Freeze()

	var queue chan func() error
	elems := make([]starlark.Value, len(g.calls))
	for i, v := range g.calls {
		var (
			i      = i
			fn     = v.fn
			args   = v.args
			kwargs = v.kwargs
		)

		if err := g.limiter.Wait(g.ctx); err != nil {
			return nil, err
		}

		call := func() error {
			thread := new(starlark.Thread)
			thread.SetLocal("context", g.ctx)

			v, err := starlark.Call(thread, fn, args, kwargs)
			if err != nil {
				return err
			}

			elems[i] = v
			return nil
		}

		if g.n <= 0 {
			g.group.Go(call)
			continue
		}

		if i == 0 {
			queue = make(chan func() error, g.n)
		}

		if i < g.n {
			g.group.Go(func() error {
				for call := range queue {
					if err := call(); err != nil {
						return err
					}
				}
				return nil
			})
		}

		select {
		case queue <- call:
		case <-g.ctx.Done():
			return nil, g.ctx.Err()
		}
	}

	if queue != nil {
		close(queue)
	}

	if err := g.group.Wait(); err != nil {
		return nil, err
	}

	return starlark.Tuple(elems), nil
}
