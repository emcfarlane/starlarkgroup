# Tests of Starlark 'group' extension.

load("assert.star", "assert")

def fibonacci(n):
    res = list(range(n))
    for i in res[2:]:
        res[i] = res[i - 2] + res[i - 1]
    return res

def multi_fib(l):
    g = group()
    for i in l:
        g.go(fibonacci, i)
    return g.wait()

assert.eq(multi_fib([1, 2, 3, 4]), ([0], [0, 1], [0, 1, 1], [0, 1, 1, 2]))

def list_elems():
    l = [1, 2, 3]
    l.append(4)
    g = group()
    g.go(len, l)
    l.append(5)  # error: mutate frozen list
    return g.wait()

def test_run_order(t):
    assert.eq(list_elems(), (5,))

def square(x):
    return x * x

def square_all(vs):
    # Create a group of 100 go routines, limited every 1ms with a burst of 2.
    g = group(n = 100, every = "1ms", burst = 2)
    for i in vs:
        g.go(square, i)  # args[1:] and kwargs passed to arg[0]
    return g.wait()

def test_square_all(t):
    assert.eq(
        square_all(range(100)),
        (0, 1, 4, 9, 16, 25, 36, 49, 64, 81, 100, 121, 144, 169, 196, 225, 256, 289, 324, 361, 400, 441, 484, 529, 576, 625, 676, 729, 784, 841, 900, 961, 1024, 1089, 1156, 1225, 1296, 1369, 1444, 1521, 1600, 1681, 1764, 1849, 1936, 2025, 2116, 2209, 2304, 2401, 2500, 2601, 2704, 2809, 2916, 3025, 3136, 3249, 3364, 3481, 3600, 3721, 3844, 3969, 4096, 4225, 4356, 4489, 4624, 4761, 4900, 5041, 5184, 5329, 5476, 5625, 5776, 5929, 6084, 6241, 6400, 6561, 6724, 6889, 7056, 7225, 7396, 7569, 7744, 7921, 8100, 8281, 8464, 8649, 8836, 9025, 9216, 9409, 9604, 9801),
    )
