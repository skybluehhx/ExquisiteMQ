package com.lin.server.DB;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author jianglinzou
 * @date 2019/3/14 下午9:45
 */
public class Result<E, A> {
    private E e;
    private A a;
    private boolean isSuccess;

    private Result(E e, A a, boolean isSuccess) {
        this.e = e;
        this.a = a;
        this.isSuccess = isSuccess;
    }

    public static <E, A> Result<E, A> success(A a) {
        return new Result<>(null, a, true);
    }

    public static <E, A> Result<E, A> fail(E e) {
        return new Result<>(e, null, false);
    }

    /**
     * Return `true` if this validation is success.
     */
    public boolean isSuccess() {
        return isSuccess;
    }

    /**
     * Return `true` if this validation is failure.
     */
    public boolean isFailure() {
        return !isSuccess();
    }

    /**
     * Catamorphism. Run the first given function if failure, otherwise, the second given function.
     */
    public <X> X fold(Function<E, X> fail, Function<A, X> succ) {
        if (isSuccess) {
            return succ.apply(a);
        } else {
            return fail.apply(e);
        }
    }

    /**
     * Catamorphism. Run the first given function if failure, otherwise, the second given function.
     */
    public void apply(Consumer<E> fail, Consumer<A> succ) {
        if (isSuccess) {
            succ.accept(a);
        } else {
            fail.accept(e);
        }
    }

    public E getError() {
        return e;
    }

    public A getSuccess() {
        return a;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}

