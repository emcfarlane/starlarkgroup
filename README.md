# starlarkgroup

[![GoDev](https://img.shields.io/static/v1?label=godev&message=reference&color=00add8)](https://pkg.go.dev/mod/github.com/emcfarlane/starlarkgroup)
![test](https://github.com/emcfarlane/starlarkgroup/actions/workflows/test.yml/badge.svg)

Go errgroup.Group for starlark. Allows a starlark thread to spawn go routines.
Each go routine can be optionally pooled and rate limited.
Arguments to go are frozen. Wait returns a tuple of sorted values in order of 
calling. Calls are lazy evaluated and only executed when `wait()` is called.

```python
def square(x):
    return x*x

def square_all(vs):
    # Create a group of 10 go routines, limited every 10ms with a burst of 10.
    g = group(n = 10, every = "10ms", burst = 10)
    for i in vs:
        g.go(square, i)  # args[1:] and kwargs passed to arg[0]
    return g.wait()

print(square_all(range(10)))  # prints: [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]
```
