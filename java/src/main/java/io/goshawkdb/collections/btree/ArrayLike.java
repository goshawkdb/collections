package io.goshawkdb.collections.btree;

import java.lang.reflect.Array;
import java.util.function.BiFunction;
import java.util.function.Function;

interface ArrayLike<T> {
    int count();

    T get(int i);

    default int copyTo(T[] arr, int i) {
        return fold((x, j) -> {
            arr[j] = x;
            return j + 1;
        }, i);
    }

    default T[] copyOut(Class<T> c) {
        @SuppressWarnings("unchecked") final T[] ts = (T[]) Array.newInstance(c, count());
        copyTo(ts, 0);
        return ts;
    }

    default <A> A fold(BiFunction<T, A, A> f, A a) {
        final int n = count();
        for (int i = 0; i < n; i++) {
            a = f.apply(get(i), a);
        }
        return a;
    }

    default T last() {
        return get(count() - 1);
    }

    default T first() {
        return get(0);
    }

    // transforms

    default <U> ArrayLike<U> map(Function<T, U> f) {
        return new Mapped<>(this, f);
    }

    default ArrayLike<T> sliceFrom(int i) {
        return new Slice<>(this, i, count());
    }

    default ArrayLike<T> sliceTo(int i) {
        return new Slice<>(this, 0, i);
    }

    default ArrayLike<T> spliceIn(int i, T t) {
        return sliceTo(i).concat(wrap(t)).concat(sliceFrom(i));
    }

    default ArrayLike<T> spliceOut(int i) {
        return sliceTo(i).concat(sliceFrom(i + 1));
    }

    default ArrayLike<T> concat(ArrayLike<T> other) {
        return new Concat<>(this, other);
    }

    default ArrayLike<T> with(int i, T value) {
        return new With<>(this, i, value);
    }

    default ArrayLike<T> withoutLast() {
        return sliceTo(count() - 1);
    }

    default ArrayLike<T> withoutFirst() {
        return sliceFrom(1);
    }

    // constructors

    @SafeVarargs  // ts may have run-time type Object[]
    static <T> ArrayLike<T> wrap(T... ts) {
        return new ArrayWrapper<>(ts);
    }

    static <T> ArrayLike<T> empty() {
        return wrap();
    }

    class Mapped<T, U> implements ArrayLike<U> {
        final ArrayLike<T> delegate;
        final Function<T, U> f;

        Mapped(ArrayLike<T> delegate, Function<T, U> f) {
            this.delegate = delegate;
            this.f = f;
        }

        @Override
        public int count() {
            return delegate.count();
        }

        @Override
        public U get(int i) {
            return f.apply(delegate.get(i));
        }
    }

    class Slice<T> implements ArrayLike<T> {
        final ArrayLike<T> delegate;
        final int from;
        final int to;

        Slice(ArrayLike<T> delegate, int from, int to) {
            if (from < 0) {
                throw new IllegalArgumentException();
            }
            this.delegate = delegate;
            final int n = delegate.count();
            this.to = to <= n ? to : n;
            this.from = from <= to ? from : to;
        }

        @Override
        public int count() {
            return to - from;
        }

        @Override
        public T get(int i) {
            if (i >= 0 && from + i < to) {
                return delegate.get(from + i);
            }
            throw new IndexOutOfBoundsException();
        }
    }

    class ArrayWrapper<T> implements ArrayLike<T> {
        final Object[] ts;  // see wrap for why this is Object[] not T[]

        ArrayWrapper(Object[] ts) {
            this.ts = ts;
        }

        @Override
        public int count() {
            return ts.length;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T get(int i) {
            return (T)ts[i];
        }
    }

    class With<T> implements ArrayLike<T> {
        final ArrayLike<T> delegate;
        final int i;
        final T t;

        With(ArrayLike<T> delegate, int i, T t) {
            this.delegate = delegate;
            this.i = i;
            this.t = t;
        }

        @Override
        public int count() {
            return delegate.count();
        }

        @Override
        public T get(int j) {
            if (j == i) {
                return t;
            }
            return delegate.get(j);
        }
    }

    class Concat<T> implements ArrayLike<T> {
        final ArrayLike<T> first;
        final ArrayLike<T> second;

        Concat(ArrayLike<T> first, ArrayLike<T> second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public int count() {
            return first.count() + second.count();
        }

        @Override
        public T get(int i) {
            final int n = first.count();
            if (i < n) {
                return first.get(i);
            }
            return second.get(i - n);
        }
    }
}