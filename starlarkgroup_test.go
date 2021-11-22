// Copyright 2020 Edward McFarlane. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package starlarkgroup

import (
	"testing"

	"github.com/emcfarlane/starlarkassert"
	"go.starlark.net/starlark"
)

func TestExecFile(t *testing.T) {
	runner := func(thread *starlark.Thread, test func()) {
		t.Logf("%s", thread.Name)
		test()
	}
	globals := starlark.StringDict{
		"group": starlark.NewBuiltin("group", Make),
	}
	starlarkassert.RunTests(t, "testdata/*.star", globals, runner)
}
