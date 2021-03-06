/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.timelock.paxos;

import java.util.Collection;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;
import com.palantir.common.annotation.Inclusive;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosValue;

/**
 * This interface is used internally to allow creating a single feign proxy where different clients can be injected
 * using {@link ClientAwarePaxosLearner} to create {@link PaxosLearner}s instead of creating a proxy per client. This
 * reduces the resource allocation when a new timelock node becomes the leader.
 */
@Path("/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
        + "/" + PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE
        + "/{client}"
        + "/learner")
public interface ClientAwarePaxosLearner {
    @POST
    @Path("learn/{seq}")
    @Consumes(MediaType.APPLICATION_JSON)
    void learn(@PathParam("client") String client, @PathParam("seq") long seq, PaxosValue val);

    @Nullable
    @GET
    @Path("learned-value/{seq}")
    @Produces(MediaType.APPLICATION_JSON)
    PaxosValue getLearnedValue(@PathParam("client") String client, @PathParam("seq") long seq);

    @Nullable
    @GET
    @Path("greatest-learned-value")
    @Produces(MediaType.APPLICATION_JSON)
    PaxosValue getGreatestLearnedValue(@PathParam("client") String client);

    @Nonnull
    @GET
    @Path("learned-values-since/{seq}")
    @Produces(MediaType.APPLICATION_JSON)
    Collection<PaxosValue> getLearnedValuesSince(
            @PathParam("client") String client,
            @PathParam("seq") @Inclusive long seq);
}
