/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import feign.Client;
import feign.Request;
import feign.Request.Options;
import feign.RequestTemplate;
import feign.Response;
import feign.RetryableException;
import feign.Retryer;
import feign.Target;

public class FailoverFeignTarget<T> implements Target<T>, Retryer {
    private static final Logger log = LoggerFactory.getLogger(FailoverFeignTarget.class);

    public static final int DEFAULT_MAX_BACKOFF_MILLIS = 3000;

    private static final double GOLDEN_RATIO = (Math.sqrt(5) + 1.0) / 2.0;

    private final ImmutableList<String> servers;
    private final Class<T> type;
    private final AtomicInteger serverIndex = new AtomicInteger();
    private final int numServersToTryBeforeFailing = 14;
    private final int fastFailoverTimeoutMillis = 10000;
    private final int maxBackoffMillis;

    private final AtomicLong failuresSinceLastSwitch = new AtomicLong();
    private final AtomicLong numFastFailoverRetries = new AtomicLong();
    private final AtomicLong numSwitches = new AtomicLong();
    private final AtomicLong startTimeOfFastFailover = new AtomicLong();

    private final ThreadLocal<Integer> mostRecentServerIndex = new ThreadLocal<>();

    public FailoverFeignTarget(Collection<String> servers, Class<T> type) {
        this(servers, DEFAULT_MAX_BACKOFF_MILLIS, type);
    }

    public FailoverFeignTarget(Collection<String> servers, int maxBackoffMillis, Class<T> type) {
        Preconditions.checkArgument(maxBackoffMillis > 0);
        this.servers = ImmutableList.copyOf(ImmutableSet.copyOf(servers));
        this.type = type;
        this.maxBackoffMillis = maxBackoffMillis;
    }

    public void sucessfulCall() {
        numFastFailoverRetries.set(0);
        numSwitches.set(0);
        failuresSinceLastSwitch.set(0);
        startTimeOfFastFailover.set(0);
    }

    @Override
    public void continueOrPropagate(RetryableException ex) {
        ExceptionRetryBehaviour retryBehaviour = ExceptionRetryBehaviour.getRetryBehaviourForException(ex);

        synchronized (this) {
            // This means that another thread has failed us over already. Do nothing.
            if (mostRecentServerIndex.get() == null || mostRecentServerIndex.get() != serverIndex.get()) {
//                checkAndHandleFailure(ex);
//                if (retryBehaviour.shouldBackoff()) {
//                    pauseForBackOff(ex);
//                }
                return;
            }

            if (retryBehaviour.isFastFailover()) {
                startTimeOfFastFailover.compareAndSet(0, System.currentTimeMillis());

                final long fastFailoverStartTime = startTimeOfFastFailover.get();
                final long currentTime = System.currentTimeMillis();
                if (fastFailoverStartTime != 0
                        && (currentTime - fastFailoverStartTime) > fastFailoverTimeoutMillis) {
                    log.error("This connection has been instructed to fast failover for {}"
                                    + " seconds without establishing a successful connection."
                                    + " The remote hosts have been in a fast failover state for too long.",
                            TimeUnit.MILLISECONDS.toSeconds(fastFailoverTimeoutMillis));
                    throw ex;
                }

                serverIndex.incrementAndGet();
                if (serverIndex.get() % servers.size() == 0) {
                    pauseForBackOff(ex, 1000);
                } else {
                    pauseForBackOff(ex, 1);
                }

                return;
            }
            startTimeOfFastFailover.set(0);

            if (retryBehaviour.shouldAlsoRetryOnOtherNodes()) {
                final int numberOfRetriesOnSameNode = retryBehaviour.numberOfRetriesOnSameNode();

                if (failuresSinceLastSwitch.incrementAndGet() >= numberOfRetriesOnSameNode) {
                    serverIndex.incrementAndGet();
                    failuresSinceLastSwitch.set(0);
                }

                if (serverIndex.get() >= numServersToTryBeforeFailing) {
                    log.error("This connection has tried {} hosts rolling across {} servers, each {} times and has failed out.",
                            numServersToTryBeforeFailing, servers.size(), numberOfRetriesOnSameNode, ex);
                    throw ex;
                }

                long pauseTime = Math.round(Math.pow(GOLDEN_RATIO,
                        serverIndex.get() * retryBehaviour.numberOfRetriesOnSameNode() + failuresSinceLastSwitch.get()));
                pauseForBackOff(ex, pauseTime);
                return;
            }

            if (retryBehaviour.shouldBackoff()) {
                pauseForBackOff(ex, 1);
            }
        }
    }

    private synchronized void pauseForBackOff(RetryableException ex, long pauseTimeInMillis) {
        long timeout = Math.min(maxBackoffMillis, pauseTimeInMillis);

        try {
            log.trace("Pausing {}ms before retrying", timeout);
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ex;
        }
    }

    @SuppressFBWarnings("CN_IDIOM_NO_SUPER_CALL")
    @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
    @Override
    public Retryer clone() {
        mostRecentServerIndex.remove();
        return this;
    }

    @Override
    public Class<T> type() {
        return type;
    }

    @Override
    public String name() {
        return "server list: " + servers;
    }

    @Override
    public String url() {
        int indexToHit = serverIndex.get();
        mostRecentServerIndex.set(indexToHit);
        return servers.get(indexToHit % servers.size());
    }

    @Override
    public Request apply(RequestTemplate input) {
        if (input.url().indexOf("http") != 0) {
            input.insert(0, url());
        }
        return input.request();
    }

    public Client wrapClient(final Client client)  {
        return new Client() {
            @Override
            public Response execute(Request request, Options options) throws IOException {
                Response response = client.execute(request, options);
                if (response.status() >= 200 && response.status() < 300) {
                    sucessfulCall();
                }
                return response;
            }
        };
    }
}
