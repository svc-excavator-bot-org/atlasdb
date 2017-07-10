/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock.lock;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class AsyncResult<T> {

    private final CompletableFuture<T> future;

    public static AsyncResult<Void> completedResult() {
        return new AsyncResult<>(CompletableFuture.completedFuture(null));
    }

    public AsyncResult() {
        this(new CompletableFuture<T>());
    }

    private AsyncResult(CompletableFuture<T> future) {
        this.future = future;
    }

    /**
     * Marks this result as completed successfully, causing {@link #isCompletedSuccessfully()} ()} to return true, and
     * {@link #get()} to return {@code value}.
     *
     * @throws {@link IllegalStateException} if this result is already completed.
     */
    public void complete(T value) {
        Preconditions.checkState(
                future.complete(value));
    }

    /**
     * Marks this result as failed, causing {@link #isFailed()} to return true, and {@link #getError()} to return {@code
     * error}.
     *
     * @throws {@link IllegalStateException} if this result is already completed.
     */
    public void fail(Throwable error) {
        Preconditions.checkState(
                future.completeExceptionally(error));
    }

    /**
     * Marks this result as timed out, causing {@link #isTimedOut()} to return true.
     *
     * @throws {@link IllegalStateException} if this result is already completed.
     */
    public void timeout() {
        Preconditions.checkState(
                future.completeExceptionally(new TimeoutException()));
    }

    /** Returns whether this result has failed. Use {@link #getError} to retrieve the associated exception. */
    public boolean isFailed() {
        return future.isCompletedExceptionally() && !isTimedOut();
    }

    /** Returns whether this result has completed successfully. */
    public boolean isCompletedSuccessfully() {
        return future.isDone() && !future.isCompletedExceptionally();
    }

    /** Returns whether this result has completed, whether successfully or unsuccessfully. */
    public boolean isComplete() {
        return future.isDone();
    }

    /** Returns whether this result has timed out (i.e., whether {@link #timeout()} has been called. */
    public boolean isTimedOut() {
        if (!future.isCompletedExceptionally()) {
            return false;
        }

        try {
            future.join();
            return false;
        } catch (Throwable e) {
            return isTimeout(e);
        }
    }

    /**
     * Returns the successfully completed value immediately.
     *
     * @throws {@link IllegalStateException} if not completed successfully.
     **/
    public T get() {
        Preconditions.checkState(isCompletedSuccessfully());
        return future.join();
    }

    /**
     * Returns the error that caused this result to fail.
     *
     * @throws {@link IllegalStateException} if not failed.
     **/
    public Throwable getError() {
        Preconditions.checkState(isFailed());
        try {
            future.join();
            throw new IllegalStateException("This result is not failed.");
        } catch (CompletionException e) {
            return e.getCause();
        }
    }

    /**
     * Executes {@code nextResult} if and when this instance completes successfully. If this instance fails or times
     * out, then {@code nextResult} is not executed.
     *
     * @return an AsyncResult that is completed when either (a) both this instance and {@code nextResult} are completed
     * successfully, or (b) either of them fails or times out. In the latter case, the returned AsyncResult will contain the error
     * or timeout status associated with the result that did not complete successfully.
     */
    public AsyncResult<Void> concatWith(Supplier<AsyncResult<Void>> nextResult) {
        return new AsyncResult<>(future.thenCompose(ignored -> nextResult.get().future));
    }

    /**
     * Tests the provided predicate against the value of this result, if this it has completed successfully. If it has
     * not yet completed, or has failed or timed out, the predicate is not executed and {@code false} is returned.
     */
    public boolean test(Predicate<T> predicateIfCompletedSuccessfully) {
        if (isCompletedSuccessfully()) {
            return predicateIfCompletedSuccessfully.test(get());
        }
        return false;
    }

    /**
     * @return an AsyncResult whose value will be set to the value of this instance transformed by {@code mapper}, if
     * and when this instance completes successfully. If this instance fails or times out, {@code mapper} is never
     * called, and the returned AsyncResult will contain the error or timeout status associated with this instance.
     */
    public <U> AsyncResult<U> map(Function<T, U> mapper) {
        return new AsyncResult<U>(future.thenApply(mapper));
    }

    public void onError(Consumer<Throwable> errorHandler) {
        future.exceptionally(error -> {
            if (!isTimeout(error)) {
                errorHandler.accept(error);
            }
            return null;
        });
    }

    public void onTimeout(Runnable timeoutHandler) {
        future.exceptionally(error -> {
            if (isTimeout(error)) {
                timeoutHandler.run();
            }
            return null;
        });
    }

    public void onComplete(Runnable completionHandler) {
        future.whenComplete((a, b) -> completionHandler.run());
    }

    private static boolean isTimeout(Throwable ex) {
        return ex instanceof TimeoutException || ex.getCause() instanceof TimeoutException;
    }

    static class TimeoutException extends RuntimeException {

    }

}