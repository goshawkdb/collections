package io.goshawkdb.collections.btree;

import java.lang.reflect.Array;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface ArrayLike<T> {
    int size();

    T get(int i);

    default ArrayLike<T> copy() {
        final int n = size();
        final Object[] arr = new Object[n];
        copyTo(0, arr, 0, n);
        return new ArrayWrapper<>(arr);
    }

    void copyTo(int srcPos, Object[] dst, int dstPos, int length);

    default T[] copyOut(Class<T> c) {
        final int n = size();
        @SuppressWarnings("unchecked") final T[] ts = (T[]) Array.newInstance(c, n);
        copyTo(0, ts, 0, n);
        return ts;
    }

    default <A> A fold(BiFunction<T, A, A> f, A a) {
        final int n = size();
        for (int i = 0; i < n; i++) {
            a = f.apply(get(i), a);
        }
        return a;
    }

    default T last() {
        return get(size() - 1);
    }

    default T first() {
        return get(0);
    }

    // transforms

    default <U> ArrayLike<U> map(Function<T, U> f) {
        return new Mapped<>(this, f);
    }

    default ArrayLike<T> sliceFrom(int i) {
        return new Slice<>(this, i, size());
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
        return sliceTo(size() - 1);
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
        final int count;

        Mapped(ArrayLike<T> delegate, Function<T, U> f) {
            this.delegate = delegate;
            this.f = f;
            this.count = delegate.size();
        }

        @Override
        public int size() {
            return count;
        }

        @Override
        public U get(int i) {
            return f.apply(delegate.get(i));
        }

        @Override
        public void copyTo(int srcPos, Object[] dst, int dstPos, int length) {
            if (srcPos + length > count) {
                throw new IndexOutOfBoundsException();
            }
            for (int i = 0; i < length; i++) {
                dst[dstPos + i] = get(srcPos + i);
            }
        }
    }

    class Slice<T> implements ArrayLike<T> {
        final ArrayLike<T> delegate;
        final int from;
        final int to;

        public Slice(ArrayLike<T> delegate, int from, int to) {
            if (from < 0) {
                throw new IllegalArgumentException();
            }
            this.delegate = delegate;
            final int n = delegate.size();
            if (to > n) {
                to = n;
            }
            this.to = to;
            this.from = from <= to ? from : to;
        }

        @Override
        public int size() {
            return to - from;
        }

        @Override
        public T get(int i) {
            if (i >= 0 && from + i < to) {
                return delegate.get(from + i);
            }
            throw new IndexOutOfBoundsException();
        }

        @Override
        public void copyTo(int srcPos, Object[] dst, int dstPos, int length) {
            if (from + srcPos + length > to) {
                throw new IndexOutOfBoundsException();
            }
            delegate.copyTo(from + srcPos, dst, dstPos, length);
        }
    }

    class ArrayWrapper<T> implements ArrayLike<T> {
        final Object[] ts;  // see wrap for why this is Object[] not T[]

        ArrayWrapper(Object[] ts) {
            this.ts = ts;
        }

        @Override
        public int size() {
            return ts.length;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T get(int i) {
            return (T)ts[i];
        }

        @Override
        public void copyTo(int srcPos, Object[] dst, int dstPos, int length) {
            System.arraycopy(ts, srcPos, dst, dstPos, length);
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
        public int size() {
            return delegate.size();
        }

        @Override
        public T get(int j) {
            if (j == i) {
                return t;
            }
            return delegate.get(j);
        }

        @Override
        public void copyTo(int srcPos, Object[] dst, int dstPos, int length) {
            delegate.copyTo(srcPos, dst, dstPos, length);
            dst[srcPos + i] = t;
        }
    }

    class Concat<T> implements ArrayLike<T> {
        final ArrayLike<T> first;
        final ArrayLike<T> second;
        final int firstCount;
        final int secondCount;

        Concat(ArrayLike<T> first, ArrayLike<T> second) {
            this.first = first;
            this.second = second;
            this.firstCount = first.size();
            this.secondCount = second.size();
        }

        @Override
        public int size() {
            return firstCount + secondCount;
        }

        @Override
        public T get(int i) {
            if (i < firstCount) {
                return first.get(i);
            }
            return second.get(i - firstCount);
        }

        @Override
        public void copyTo(int srcPos, Object[] dst, int dstPos, int length) {
            if (srcPos >= firstCount) {
                second.copyTo(srcPos - firstCount, dst, dstPos, length);
                return;
            }
            final int l1 = firstCount - srcPos;
            final int l2 = l1 > length ? length : l1;
            first.copyTo(srcPos, dst, dstPos, l2);
            if (length > l2) {
                second.copyTo(0, dst, dstPos + l2, length - l2);
            }
        }
    }
}