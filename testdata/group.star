# Tests of Starlark 'group' extension.

load("assert.star", "assert")


def fibonacci(n):
    res = list(range(n))
    for i in res[2:]:
        res[i] = res[i-2] + res[i-1]
    return res


def multi_fib(l):
    g = group()
    for i in l:
        g.go(fibonacci, i)
    return g.wait()


assert.eq(multi_fib([1, 2, 3, 4]), [[0], [0, 1], [0, 1, 1], [0, 1, 1, 2]])


def test_frozen():
    l = [1, 2, 3]
    l.append(4)
    g = group()
    g.go(len, l)
    l.append(5)  # error: mutate frozen list
    g.wait()


assert.fails(test_frozen, "append: cannot append to frozen list")

def square(x):
    return x*x

def square_all(vs):
    # Create a group of 10 go routines, limited every 10ms with a burst of 10.
    g = group(n = 100, every = "1ms", burst = 2)
    for i in vs:
        g.go(square, i)  # args[1:] and kwargs passed to arg[0]
    return g.wait()

assert.eq(square_all(range(100)), [])
