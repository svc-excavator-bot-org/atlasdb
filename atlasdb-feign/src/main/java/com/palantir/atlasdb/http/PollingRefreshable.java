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

package com.palantir.atlasdb.http;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.remoting3.ext.refresh.Refreshable;

/**
 * A PollingRefreshable serves as a bridge between a {@link Supplier} and {@link Refreshable}, polling for changes
 * in the value of the Supplier and, if detecting a change, pushing it to the linked Refreshable.
 *
 * @param <T> type of the value supplied / pushed to the Refreshable
 */
public final class PollingRefreshable<T> implements AutoCloseable {
    // TODO (jkong): Should this be configurable?
    @VisibleForTesting
    static final Duration POLL_INTERVAL = Duration.ofSeconds(5L);

    private static final Logger log = LoggerFactory.getLogger(PollingRefreshable.class);

    private final Supplier<T> supplier;
    private final ScheduledExecutorService poller;

    private final Refreshable<T> refreshable = Refreshable.empty();

    private volatile T lastSeenValue;

    private PollingRefreshable(Supplier<T> supplier, ScheduledExecutorService poller) {
        this.supplier = supplier;
        this.poller = poller;

        try {
            lastSeenValue = supplier.get();
            refreshable.set(lastSeenValue);
        } catch (Exception e) {
            log.info("Exception occurred in supplier when trying to populate the initial value.");
            lastSeenValue = null;
        }
    }

    public static <T> PollingRefreshable<T> create(Supplier<T> supplier) {
        return createWithSpecificPoller(supplier,
                Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("polling-refreshable", true)));
    }

    @VisibleForTesting
    static <T> PollingRefreshable<T> createWithSpecificPoller(Supplier<T> supplier, ScheduledExecutorService poller) {
        PollingRefreshable<T> pollingRefreshable = new PollingRefreshable<>(supplier, poller);
        pollingRefreshable.scheduleUpdates();
        return pollingRefreshable;
    }

    public Refreshable<T> getRefreshable() {
        return refreshable;
    }

    private void scheduleUpdates() {
        poller.scheduleAtFixedRate(() -> {
            try {
                T value = supplier.get();
                if (!value.equals(lastSeenValue)) {
                    lastSeenValue = value;
                    refreshable.set(lastSeenValue);
                }
            } catch (Exception e) {
                log.info("Exception occurred in supplier when polling for a new value in our PollingRefreshable."
                        + " The last value we saw was {}.",
                        UnsafeArg.of("currentValue", lastSeenValue),
                        e);
            }
        }, POLL_INTERVAL.getSeconds(), POLL_INTERVAL.getSeconds(), TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        poller.shutdown();
    }
}