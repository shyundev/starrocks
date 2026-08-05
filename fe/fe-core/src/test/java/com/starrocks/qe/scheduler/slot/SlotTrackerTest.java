// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.qe.scheduler.slot;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.WarehouseManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class SlotTrackerTest {
    private static SlotManager slotManager;

    @BeforeAll
    public static void beforeClass() {
        MetricRepo.init();
        ResourceUsageMonitor resourceUsageMonitor = new ResourceUsageMonitor();
        slotManager = new SlotManager(resourceUsageMonitor);
    }

    @Test
    public void testRequireSlot() {
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of());

        LogicalSlot slot1 = generateSlot(1);
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        assertThat(slotTracker.getSlots()).hasSize(1);
        // Re-require the same slot has no effect.
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        assertThat(slotTracker.getSlots()).hasSize(1);
    }

    @Test
    public void tesAllocateSlot() {
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of());

        LogicalSlot slot1 = generateSlot(1);

        // Allocation of a slot that has not been required has no effect.
        slotTracker.allocateSlot(slot1);
        assertThat(slotTracker.getSlots()).isEmpty();

        // Allocation of a required slot has effect.
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        slotTracker.allocateSlot(slot1);
        assertThat(slotTracker.getNumAllocatedSlots()).isOne();

        // Re-allocation of the same slot has no effect.
        slotTracker.allocateSlot(slot1);
        assertThat(slotTracker.getNumAllocatedSlots()).isOne();
    }

    @Test
    public void tesReleaseSlot() {
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of());

        LogicalSlot slot1 = generateSlot(1);

        // 1. Release of a slot that has not been required has no effect.
        assertThat(slotTracker.releaseSlot(slot1.getSlotId())).isNull();

        // 2.1 Release a required slot.
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        assertThat(slotTracker.releaseSlot(slot1.getSlotId())).isSameAs(slot1);
        assertThat(slotTracker.getNumAllocatedSlots()).isZero();
        assertThat(slotTracker.getSlots()).isEmpty();

        // 2.2 Re-release of the same slot has no effect.
        slotTracker.allocateSlot(slot1);
        assertThat(slotTracker.releaseSlot(slot1.getSlotId())).isNull();

        // 3.1 Release a required slot.
        assertThat(slotTracker.requireSlot(slot1)).isTrue();

        slotTracker.allocateSlot(slot1);
        assertThat(slotTracker.getNumAllocatedSlots()).isOne();

        assertThat(slotTracker.releaseSlot(slot1.getSlotId())).isSameAs(slot1);
        assertThat(slotTracker.getNumAllocatedSlots()).isZero();
        assertThat(slotTracker.getSlots()).isEmpty();

        // 3.2 Re-release of the same slot has no effect.
        slotTracker.allocateSlot(slot1);
        assertThat(slotTracker.releaseSlot(slot1.getSlotId())).isNull();
    }

    private static LogicalSlot generateSlot(int numSlots) {
        return new LogicalSlot(UUIDUtil.genTUniqueId(), "fe", WarehouseManager.DEFAULT_WAREHOUSE_ID,
                LogicalSlot.ABSENT_GROUP_ID, numSlots, 0, 0, 0,
                0, 0);
    }

    @Test
    public void testSlotTrackerMetrics() {
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of());
        assertThat(slotTracker.getWarehouseId()).isEqualTo(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        assertThat(slotTracker.getWarehouseName().equals(""));
        assertThat(slotTracker.getQueuePendingLength()).isEqualTo(0);
        assertThat(slotTracker.getNumAllocatedSlots()).isEqualTo(0);
        assertThat(slotTracker.getMaxRequiredSlots()).isEmpty();
        assertThat(slotTracker.getSumRequiredSlots()).isEmpty();
        assertThat(slotTracker.getMaxSlots()).isEmpty();
        assertThat(slotTracker.getRemainSlots()).isEqualTo(slotTracker.getMaxSlots());
        assertThat(slotTracker.getMaxQueueQueueLength()).isEqualTo(GlobalVariable.getQueryQueueMaxQueuedQueries());
        assertThat(slotTracker.getMaxQueuePendingTimeSecond()).isEqualTo(GlobalVariable.getQueryQueuePendingTimeoutSecond());

        LogicalSlot slot1 = generateSlot(1);
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        assertThat(slotTracker.getQueuePendingLength()).isEqualTo(1);
        assertThat(slotTracker.getNumAllocatedSlots()).isEqualTo(0);
        assertThat(slotTracker.getMaxRequiredSlots()).isEqualTo(Optional.of(1));
        assertThat(slotTracker.getSumRequiredSlots()).isEqualTo(Optional.of(1));
        assertThat(slotTracker.getMaxSlots()).isEmpty();
        assertThat(slotTracker.getRemainSlots()).isEmpty();
        assertThat(slotTracker.getMaxQueueQueueLength()).isEqualTo(GlobalVariable.getQueryQueueMaxQueuedQueries());
        assertThat(slotTracker.getMaxQueuePendingTimeSecond()).isEqualTo(GlobalVariable.getQueryQueuePendingTimeoutSecond());

        // re-release of the same slot has no effect.
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        assertThat(slotTracker.getQueuePendingLength()).isEqualTo(1);
        assertThat(slotTracker.getNumAllocatedSlots()).isEqualTo(0);
        assertThat(slotTracker.getMaxRequiredSlots()).isEqualTo(Optional.of(1));
        assertThat(slotTracker.getSumRequiredSlots()).isEqualTo(Optional.of(1));
        assertThat(slotTracker.getMaxSlots()).isEmpty();
        assertThat(slotTracker.getRemainSlots()).isEmpty();
        assertThat(slotTracker.getMaxQueueQueueLength()).isEqualTo(GlobalVariable.getQueryQueueMaxQueuedQueries());
        assertThat(slotTracker.getMaxQueuePendingTimeSecond()).isEqualTo(GlobalVariable.getQueryQueuePendingTimeoutSecond());

        // allocate slot
        slotTracker.allocateSlot(slot1);
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        assertThat(slotTracker.getQueuePendingLength()).isEqualTo(0);
        assertThat(slotTracker.getNumAllocatedSlots()).isEqualTo(1);
        assertThat(slotTracker.getMaxRequiredSlots()).isEmpty();
        assertThat(slotTracker.getSumRequiredSlots()).isEmpty();
        assertThat(slotTracker.getMaxSlots()).isEmpty();
        assertThat(slotTracker.getRemainSlots()).isEmpty();
        assertThat(slotTracker.getMaxQueueQueueLength()).isEqualTo(GlobalVariable.getQueryQueueMaxQueuedQueries());
        assertThat(slotTracker.getMaxQueuePendingTimeSecond()).isEqualTo(GlobalVariable.getQueryQueuePendingTimeoutSecond());

        // release slot tracker
        assertThat(slotTracker.releaseSlot(slot1.getSlotId())).isSameAs(slot1);
        assertThat(slotTracker.getQueuePendingLength()).isEqualTo(0);
        assertThat(slotTracker.getNumAllocatedSlots()).isEqualTo(0);
        assertThat(slotTracker.getMaxRequiredSlots()).isEmpty();
        assertThat(slotTracker.getSumRequiredSlots()).isEmpty();
        assertThat(slotTracker.getMaxSlots()).isEmpty();
        assertThat(slotTracker.getRemainSlots()).isEqualTo(slotTracker.getMaxSlots());
        assertThat(slotTracker.getMaxQueueQueueLength()).isEqualTo(GlobalVariable.getQueryQueueMaxQueuedQueries());
        assertThat(slotTracker.getMaxQueuePendingTimeSecond()).isEqualTo(GlobalVariable.getQueryQueuePendingTimeoutSecond());
    }

    @Test
    public void testExtraMessage() {
        QueryQueueOptions.V2 v2 = new QueryQueueOptions.V2(
                GlobalVariable.getQueryQueueMaxQueuedQueries(),
                GlobalVariable.getQueryQueuePendingTimeoutSecond(),
                1, 1, 1, 1
        );
        BaseSlotTracker.ExtraMessage extraMessage = new BaseSlotTracker.ExtraMessage(1, v2);
        assertThat(extraMessage.getConcurrency()).isEqualTo(1);
        assertThat(extraMessage.getV2()).isEqualTo(v2);
        String json = GsonUtils.GSON.toJson(extraMessage);
        assertThat(json).isEqualTo("{\"Concurrency\":1,\"QueryQueueOption\":"
                + GsonUtils.GSON.toJson(v2) + "}");
    }

    @Test
    public void testGetEarliestQueryWaitTimeSecond() {
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of());
        LogicalSlot slot1 = generateSlot(1);
        slotTracker.requireSlot(slot1);
        Uninterruptibles.sleepUninterruptibly(1000, java.util.concurrent.TimeUnit.MILLISECONDS);

        // wait time is greater than 1 second before slot allocation
        double waitTime = slotTracker.getEarliestQueryWaitTimeSecond();
        assertThat(waitTime).isGreaterThanOrEqualTo(1.0);

        // wait time is zero after slot allocation
        slotTracker.allocateSlot(slot1);
        waitTime = slotTracker.getEarliestQueryWaitTimeSecond();
        assertThat(waitTime).isEqualTo(0.0);
    }

    @Test
    public void testPeakExpiredSlotsWithDifferentQueryTimeout() {
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of());

        long nowMs = System.currentTimeMillis();
        // The slot with a long query_timeout has the earliest pending deadline but is not expired yet.
        LogicalSlot longQuerySlot = generateSlot(nowMs - 10_000, nowMs + 600_000);
        LogicalSlot shortQuerySlot = generateSlot(nowMs - 5_000, nowMs - 1_000);
        LogicalSlot shorterQuerySlot = generateSlot(nowMs - 3_000, nowMs - 2_000);

        assertThat(slotTracker.requireSlot(longQuerySlot)).isTrue();
        assertThat(slotTracker.requireSlot(shortQuerySlot)).isTrue();
        assertThat(slotTracker.requireSlot(shorterQuerySlot)).isTrue();
        slotTracker.allocateSlot(longQuerySlot);
        slotTracker.allocateSlot(shortQuerySlot);
        slotTracker.allocateSlot(shorterQuerySlot);

        // The whole expired prefix is peaked in one scan, in the order the slots expire,
        // even though the slot which is not expired yet has an earlier pending deadline.
        assertThat(slotTracker.peakExpiredSlots()).containsExactly(shorterQuerySlot, shortQuerySlot);

        // After the expired slots are released, the scan is empty and the next deadline is in the future,
        // which is the precondition the slot manager worker relies on to block instead of looping.
        slotTracker.releaseSlot(shorterQuerySlot.getSlotId());
        slotTracker.releaseSlot(shortQuerySlot.getSlotId());
        assertThat(slotTracker.peakExpiredSlots()).isEmpty();
        assertThat(slotTracker.getMinExpiredTimeMs()).isGreaterThan(System.currentTimeMillis());
    }

    @Test
    public void testMinExpiredTimeMs() {
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of());
        assertThat(slotTracker.getMinExpiredTimeMs()).isZero();

        long nowMs = System.currentTimeMillis();
        LogicalSlot slot = generateSlot(nowMs - 10_000, nowMs + 600_000);
        assertThat(slotTracker.requireSlot(slot)).isTrue();
        slotTracker.allocateSlot(slot);

        // The pending deadline has passed, so only the allocated deadline can tell when the slot expires.
        assertThat(slotTracker.getMinExpiredTimeMs()).isEqualTo(slot.getExpiredAllocatedTimeMs());
        assertThat(slotTracker.peakExpiredSlots()).isEmpty();
    }

    private static LogicalSlot generateSlot(long expiredPendingTimeMs, long expiredAllocatedTimeMs) {
        return new LogicalSlot(UUIDUtil.genTUniqueId(), "fe", WarehouseManager.DEFAULT_WAREHOUSE_ID,
                LogicalSlot.ABSENT_GROUP_ID, 1, expiredPendingTimeMs, expiredAllocatedTimeMs, 0,
                0, 0);
    }
}
