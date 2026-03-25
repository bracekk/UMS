(function () {
  const LANE_STEP = 56;
  const JOB_TOP_OFFSET = 8;

  let plannerBusy = false;
  let suppressClickUntil = 0;
  let tooltipEl = null;
  let activeTooltipTarget = null;
  let activePopover = null;

  let dragAutoScrollRaf = null;
  let isDraggingPlannerJob = false;
  let lastDragClientX = 0;
  let lastDragClientY = 0;
  let activeDraggedCard = null;
  let activeDraggedWrap = null;

  function ensureTooltip() {
    if (tooltipEl) return tooltipEl;

    tooltipEl = document.createElement("div");
    tooltipEl.className = "planner-job-tooltip";
    tooltipEl.hidden = true;
    document.body.appendChild(tooltipEl);
    return tooltipEl;
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function showTooltip(target) {
    const tooltip = ensureTooltip();

    const order = escapeHtml(target.dataset.tooltipOrder || "-");
    const job = escapeHtml(target.dataset.tooltipJob || "-");
    const product = escapeHtml(target.dataset.tooltipProduct || "-");
    const seq = escapeHtml(target.dataset.tooltipSequence || "-");
    const remaining = escapeHtml(target.dataset.tooltipRemaining || "-");

    tooltip.innerHTML = `
      <div class="planner-job-tooltip-title">${job}</div>
      <div class="planner-job-tooltip-line"><span>Order</span><strong>${order}</strong></div>
      <div class="planner-job-tooltip-line"><span>Product</span><strong>${product}</strong></div>
      <div class="planner-job-tooltip-line"><span>Sequence</span><strong>${seq}</strong></div>
      <div class="planner-job-tooltip-line"><span>Remaining</span><strong>${remaining}</strong></div>
    `;

    tooltip.hidden = false;
    activeTooltipTarget = target;
    positionTooltip(target);
  }

  function positionTooltip(target) {
    if (!tooltipEl || tooltipEl.hidden || !target) return;

    const rect = target.getBoundingClientRect();
    const tooltipRect = tooltipEl.getBoundingClientRect();

    let top = rect.top - tooltipRect.height - 12;
    let left = rect.left;

    if (left + tooltipRect.width > window.innerWidth - 16) {
      left = window.innerWidth - tooltipRect.width - 16;
    }

    if (left < 16) {
      left = 16;
    }

    if (top < 12) {
      top = rect.bottom + 12;
    }

    tooltipEl.style.top = `${top + window.scrollY}px`;
    tooltipEl.style.left = `${left + window.scrollX}px`;
  }

  function hideTooltip() {
    if (tooltipEl) tooltipEl.hidden = true;
    activeTooltipTarget = null;
  }

  function setPlannerBusyState(isBusy) {
    plannerBusy = isBusy;
    document.documentElement.classList.toggle("planner-is-saving", isBusy);
    document.body.classList.toggle("planner-is-saving", isBusy);
  }

  function parseIsoDate(value) {
    if (!value) return null;
    const parts = String(value).split("-").map(Number);
    if (parts.length !== 3) return null;
    return new Date(parts[0], parts[1] - 1, parts[2]);
  }

  function dayDiff(a, b) {
    const ms = 24 * 60 * 60 * 1000;
    return Math.round((b.getTime() - a.getTime()) / ms);
  }

  function getVisibleMonthBounds() {
    const dayCells = Array.from(document.querySelectorAll(".planner-day-cell[data-date]"));
    if (!dayCells.length) return null;

    const first = parseIsoDate(dayCells[0].dataset.date);
    const last = parseIsoDate(dayCells[dayCells.length - 1].dataset.date);

    if (!first || !last) return null;

    return {
      first,
      last,
      slotCount: dayCells.length
    };
  }

  function buildVisiblePlacement(plannedStart, plannedEnd) {
    const bounds = getVisibleMonthBounds();
    if (!bounds) return null;

    const start = parseIsoDate(plannedStart);
    const end = parseIsoDate(plannedEnd);

    if (!start || !end) return null;
    if (end < bounds.first || start > bounds.last) return null;

    const clampedStart = start < bounds.first ? bounds.first : start;
    const clampedEnd = end > bounds.last ? bounds.last : end;

    const startDay = dayDiff(bounds.first, clampedStart) + 1;
    const endDay = dayDiff(bounds.first, clampedEnd) + 1;
    const spanDays = Math.max(1, (endDay - startDay) + 1);

    return {
      startDay,
      endDay,
      spanDays
    };
  }

  function getBarsLayerByWorkstationId(workstationId) {
    return document.querySelector(`.planner-bars-layer[data-workstation-id="${workstationId}"]`);
  }

  function getRowTrackByWorkstationId(workstationId) {
    return document.querySelector(`.planner-row-track[data-workstation-id="${workstationId}"]`);
  }

  function getWorkstationCellByWorkstationId(workstationId) {
    return document.querySelector(`.planner-workstation-cell[data-workstation-id="${workstationId}"]`);
  }

  function getJobWrapById(jobId) {
    return document.querySelector(`.planner-job-wrap[data-job-id="${jobId}"]`);
  }

  function getMonthSlotCount() {
    return document.querySelectorAll(".planner-day-cell[data-date]").length || 31;
  }

  function syncWrapGeometry(wrap) {
    const slotCount = getMonthSlotCount();
    const startDay = Number(wrap.dataset.startDay || 1);
    const spanDays = Number(wrap.dataset.spanDays || 1);
    const laneIndex = Number(wrap.dataset.laneIndex || 0);

    wrap.style.left = `calc((100% / ${slotCount}) * (${startDay} - 1) + 4px)`;
    wrap.style.width = `calc((100% / ${slotCount}) * ${spanDays} - 8px)`;
    wrap.style.top = `calc((${laneIndex} * ${LANE_STEP}px) + ${JOB_TOP_OFFSET}px)`;
  }

  function updateLaneBadge(workstationId, laneCount) {
    const cell = getWorkstationCellByWorkstationId(workstationId);
    if (!cell) return;

    cell.style.setProperty("--planner-lanes", String(laneCount));

    const badge = cell.querySelector(".planner-workstation-lanes");
    if (badge) {
      badge.textContent = `${laneCount} lane${laneCount !== 1 ? "s" : ""}`;
    }
  }

  function updateRowHeight(workstationId, laneCount) {
    const rowTrack = getRowTrackByWorkstationId(workstationId);
    const cell = getWorkstationCellByWorkstationId(workstationId);
    if (rowTrack) rowTrack.style.setProperty("--planner-lanes", String(laneCount));
    if (cell) cell.style.setProperty("--planner-lanes", String(laneCount));
  }

  function assignLanesForWorkstation(workstationId) {
    const barsLayer = getBarsLayerByWorkstationId(workstationId);
    if (!barsLayer) return;

    const wraps = Array.from(barsLayer.querySelectorAll(".planner-job-wrap"));

    wraps.sort((a, b) => {
      const aStart = Number(a.dataset.startDay || 1);
      const bStart = Number(b.dataset.startDay || 1);
      if (aStart !== bStart) return aStart - bStart;

      const aEnd = Number(a.dataset.endDay || aStart);
      const bEnd = Number(b.dataset.endDay || bStart);
      if (aEnd !== bEnd) return aEnd - bEnd;

      return Number(a.dataset.jobId || 0) - Number(b.dataset.jobId || 0);
    });

    const lanes = [];

    wraps.forEach((wrap) => {
      const startDay = Number(wrap.dataset.startDay || 1);
      const endDay = Number(wrap.dataset.endDay || startDay);

      let laneIndex = 0;
      while (laneIndex < lanes.length && startDay <= lanes[laneIndex]) {
        laneIndex += 1;
      }

      lanes[laneIndex] = endDay;
      wrap.dataset.laneIndex = String(laneIndex);
      syncWrapGeometry(wrap);
    });

    const laneCount = Math.max(1, lanes.length);
    updateLaneBadge(workstationId, laneCount);
    updateRowHeight(workstationId, laneCount);
  }

  function portalizePopover(popover) {
    if (!popover || popover.dataset.portalized === "1") return;
    document.body.appendChild(popover);
    popover.dataset.portalized = "1";
  }

  function positionPopover(popover, anchor) {
    if (!popover || !anchor) return;

    const rect = anchor.getBoundingClientRect();
    popover.style.position = "absolute";
    popover.style.top = "0px";
    popover.style.left = "0px";
    popover.hidden = false;

    const popRect = popover.getBoundingClientRect();
    const gap = 12;

    let top = rect.bottom + gap + window.scrollY;
    let left = rect.left + window.scrollX;

    if (left + popRect.width > window.innerWidth - 16 + window.scrollX) {
      left = window.innerWidth - popRect.width - 16 + window.scrollX;
    }

    if (left < window.scrollX + 16) {
      left = window.scrollX + 16;
    }

    if (top + popRect.height > window.scrollY + window.innerHeight - 16) {
      top = rect.top - popRect.height - gap + window.scrollY;
    }

    if (top < window.scrollY + 16) {
      top = window.scrollY + 16;
    }

    popover.style.top = `${top}px`;
    popover.style.left = `${left}px`;
  }

  function openPopover(openBtn) {
    const id = openBtn.dataset.openSplit;
    const pop = document.getElementById(id);
    const wrap = openBtn.closest(".planner-job-wrap, .planner-unscheduled-card");

    if (!pop) return;

    closeAllPopovers();

    portalizePopover(pop);
    positionPopover(pop, openBtn);
    pop.hidden = false;

    if (wrap) wrap.classList.add("is-popover-open");
    activePopover = pop;
  }

  function closeAllPopovers() {
    document.querySelectorAll(".planner-split-popover").forEach((pop) => {
      pop.hidden = true;
    });

    document.querySelectorAll(".is-popover-open").forEach((el) => {
      el.classList.remove("is-popover-open");
    });

    activePopover = null;
  }

    function buildSplitRow(selectHtml) {
    const row = document.createElement("div");
    row.className = "split-row";
    row.innerHTML = `
      ${selectHtml}
      <input type="number" name="split_quantity" step="0.01" min="0.01" placeholder="Qty" required>
      <button type="button" class="planner-row-remove" data-remove-split-row aria-label="Remove row">−</button>
    `;
    return row;
  }

  async function submitSplit(form) {
    const res = await fetch(form.action, {
      method: "POST",
      headers: { "X-Requested-With": "XMLHttpRequest" },
      body: new FormData(form)
    });

    const data = await res.json();

    if (!data.ok) {
      throw new Error(data.error || "Split failed");
    }

    window.location.reload();
  }

  async function updatePlannerJob(jobId, plannedStart, workstationId) {
    const formData = new FormData();
    formData.append("planned_start", plannedStart);
    formData.append("workstation_id", workstationId);

    const res = await fetch(`/planner/update-job-date/${jobId}`, {
      method: "POST",
      headers: { "X-Requested-With": "XMLHttpRequest" },
      body: formData
    });

    const data = await res.json();

    if (!data.ok) {
      throw new Error(data.error || "Planner update failed");
    }

    return data;
  }

  function applyLiveScheduledMove(jobData) {
    const wrap = getJobWrapById(jobData.id);
    if (!wrap) {
      window.location.reload();
      return;
    }

    const currentWorkstationId = wrap.dataset.workstationId;
    const targetWorkstationId = String(jobData.workstation_id);

    const placement = buildVisiblePlacement(jobData.planned_start, jobData.planned_end);
    if (!placement) {
      window.location.reload();
      return;
    }

    const targetLayer = getBarsLayerByWorkstationId(targetWorkstationId);
    if (!targetLayer) {
      window.location.reload();
      return;
    }

    wrap.dataset.workstationId = targetWorkstationId;
    wrap.dataset.startDay = String(placement.startDay);
    wrap.dataset.endDay = String(placement.endDay);
    wrap.dataset.spanDays = String(placement.spanDays);
    wrap.dataset.plannedStart = jobData.planned_start;
    wrap.dataset.plannedEnd = jobData.planned_end;

    const bar = wrap.querySelector(".planner-job-bar");
    if (bar) {
      bar.dataset.workstationId = targetWorkstationId;
      bar.dataset.startDate = jobData.planned_start;
    }

    targetLayer.appendChild(wrap);

    assignLanesForWorkstation(currentWorkstationId);
    assignLanesForWorkstation(targetWorkstationId);
  }

  function createCustomSelect(nativeSelect) {
    if (!nativeSelect || nativeSelect.dataset.enhanced === "1") return;

    nativeSelect.dataset.enhanced = "1";
    nativeSelect.classList.add("planner-native-select-hidden");

    const wrapper = document.createElement("div");
    wrapper.className = "planner-select";
    wrapper.tabIndex = 0;

    const trigger = document.createElement("button");
    trigger.type = "button";
    trigger.className = "planner-select-trigger";
    trigger.setAttribute("aria-haspopup", "listbox");
    trigger.setAttribute("aria-expanded", "false");

    const label = document.createElement("span");
    label.className = "planner-select-label";

    const chevron = document.createElement("span");
    chevron.className = "planner-select-chevron";
    chevron.innerHTML = `
      <svg viewBox="0 0 20 20" fill="none" aria-hidden="true">
        <path d="M5 7.5L10 12.5L15 7.5" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"></path>
      </svg>
    `;

    trigger.appendChild(label);
    trigger.appendChild(chevron);

    const menu = document.createElement("div");
    menu.className = "planner-select-menu";
    menu.setAttribute("role", "listbox");

    const syncLabel = () => {
      const option = nativeSelect.options[nativeSelect.selectedIndex];
      label.textContent = option ? option.textContent : "Select";
      wrapper.classList.toggle("is-placeholder", !nativeSelect.value);
    };

    Array.from(nativeSelect.options).forEach((option) => {
      const item = document.createElement("button");
      item.type = "button";
      item.className = "planner-select-option";
      item.setAttribute("role", "option");
      item.dataset.value = option.value;
      item.textContent = option.textContent;

      item.addEventListener("click", function (e) {
        e.preventDefault();
        nativeSelect.value = option.value;
        nativeSelect.dispatchEvent(new Event("change", { bubbles: true }));
        menu.querySelectorAll(".planner-select-option").forEach((opt) => {
          opt.classList.toggle("is-selected", opt.dataset.value === option.value);
        });
        closeCustomSelect(wrapper);
        syncLabel();
      });

      menu.appendChild(item);
    });

    nativeSelect.parentNode.insertBefore(wrapper, nativeSelect);
    wrapper.appendChild(trigger);
    wrapper.appendChild(menu);
    wrapper.appendChild(nativeSelect);

    const syncSelectedState = () => {
      menu.querySelectorAll(".planner-select-option").forEach((opt) => {
        opt.classList.toggle("is-selected", opt.dataset.value === nativeSelect.value);
      });
      syncLabel();
    };

    nativeSelect.addEventListener("change", syncSelectedState);

    trigger.addEventListener("click", function (e) {
      e.preventDefault();
      e.stopPropagation();

      const isOpen = wrapper.classList.contains("is-open");
      closeAllCustomSelects();

      if (!isOpen) {
        wrapper.classList.add("is-open");
        trigger.setAttribute("aria-expanded", "true");
      }
    });

    wrapper.addEventListener("keydown", function (e) {
      if (e.key === "Escape") {
        closeCustomSelect(wrapper);
        return;
      }

      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        if (!wrapper.classList.contains("is-open")) {
          closeAllCustomSelects();
          wrapper.classList.add("is-open");
          trigger.setAttribute("aria-expanded", "true");
        }
      }
    });

    syncSelectedState();
  }

  function closeCustomSelect(wrapper) {
    if (!wrapper) return;
    wrapper.classList.remove("is-open");
    const trigger = wrapper.querySelector(".planner-select-trigger");
    if (trigger) trigger.setAttribute("aria-expanded", "false");
  }

  function closeAllCustomSelects() {
    document.querySelectorAll(".planner-select.is-open").forEach(closeCustomSelect);
  }

  function enhanceCustomSelects(root) {
    (root || document).querySelectorAll("select.planner-custom-select, select.planner-native-select").forEach(createCustomSelect);
  }

  function startDragAutoScroll() {
    if (dragAutoScrollRaf) return;

    const tick = () => {
      if (!isDraggingPlannerJob) {
        dragAutoScrollRaf = null;
        return;
      }

      const viewportHeight = window.innerHeight;
      const viewportWidth = window.innerWidth;

      const edgeThresholdY = 190;
      const edgeThresholdX = 150;

      const maxVerticalSpeed = 72;
      const maxHorizontalSpeed = 42;

      let deltaY = 0;
      let deltaX = 0;

      if (lastDragClientY < edgeThresholdY) {
        const intensity = (edgeThresholdY - lastDragClientY) / edgeThresholdY;
        deltaY = -Math.ceil(maxVerticalSpeed * Math.pow(intensity, 1.25));
      } else if (lastDragClientY > viewportHeight - edgeThresholdY) {
        const intensity = (lastDragClientY - (viewportHeight - edgeThresholdY)) / edgeThresholdY;
        deltaY = Math.ceil(maxVerticalSpeed * Math.pow(intensity, 1.25));
      }

      if (lastDragClientX < edgeThresholdX) {
        const intensity = (edgeThresholdX - lastDragClientX) / edgeThresholdX;
        deltaX = -Math.ceil(maxHorizontalSpeed * Math.pow(intensity, 1.2));
      } else if (lastDragClientX > viewportWidth - edgeThresholdX) {
        const intensity = (lastDragClientX - (viewportWidth - edgeThresholdX)) / edgeThresholdX;
        deltaX = Math.ceil(maxHorizontalSpeed * Math.pow(intensity, 1.2));
      }

      if (deltaY !== 0) {
        window.scrollBy(0, deltaY);
      }

      const plannerScroll = document.querySelector(".planner-scroll");
      if (plannerScroll && deltaX !== 0) {
        plannerScroll.scrollLeft += deltaX;
      }

      dragAutoScrollRaf = requestAnimationFrame(tick);
    };

    dragAutoScrollRaf = requestAnimationFrame(tick);
  }

  function stopDragAutoScroll() {
    isDraggingPlannerJob = false;
    if (dragAutoScrollRaf) {
      cancelAnimationFrame(dragAutoScrollRaf);
      dragAutoScrollRaf = null;
    }
  }

  function bindPlannerDragAutoScrollTracking() {
    document.addEventListener("dragover", function (e) {
      if (!isDraggingPlannerJob) return;

      lastDragClientX = e.clientX;
      lastDragClientY = e.clientY;

      const slot = e.target.closest(".planner-day-slot");
      document.querySelectorAll(".planner-day-slot.drag-over").forEach((el) => {
        if (el !== slot) el.classList.remove("drag-over");
      });

      if (slot) {
        slot.classList.add("drag-over");
      }
    });

    document.addEventListener("drop", function () {
      stopDragAutoScroll();
      document.querySelectorAll(".planner-day-slot.drag-over").forEach((slot) => {
        slot.classList.remove("drag-over");
      });
    });

    document.addEventListener("dragend", function () {
      stopDragAutoScroll();
      document.querySelectorAll(".planner-day-slot.drag-over").forEach((slot) => {
        slot.classList.remove("drag-over");
      });
    });
  }

  function bindPlannerDrag() {
    document.querySelectorAll(".planner-job-wrap, .planner-unscheduled-card").forEach((card) => {
      if (card.dataset.dragBound === "1") return;
      card.dataset.dragBound = "1";

      card.addEventListener("dragstart", function (e) {
        if (e.target.closest("button, a, input, select, textarea, .planner-split-popover")) {
          e.preventDefault();
          return;
        }

        e.dataTransfer.effectAllowed = "move";
        e.dataTransfer.setData("jobId", card.dataset.jobId || "");
        suppressClickUntil = Date.now() + 800;
        closeAllPopovers();
        hideTooltip();

        activeDraggedCard = card;
        activeDraggedWrap = card.classList.contains("planner-job-wrap")
          ? card
          : card.closest(".planner-job-wrap");

        isDraggingPlannerJob = true;
        lastDragClientX = e.clientX || 0;
        lastDragClientY = e.clientY || 0;
        startDragAutoScroll();

        requestAnimationFrame(() => {
          card.classList.add("is-dragging");
        });
      });

      card.addEventListener("dragend", function () {
        card.classList.remove("is-dragging");
        stopDragAutoScroll();

        document.querySelectorAll(".planner-day-slot.drag-over").forEach((slot) => {
          slot.classList.remove("drag-over");
        });

        activeDraggedCard = null;
        activeDraggedWrap = null;
      });
    });

    document.querySelectorAll(".planner-day-slot").forEach((slot) => {
      if (slot.dataset.dropBound === "1") return;
      slot.dataset.dropBound = "1";

      slot.addEventListener("dragover", function (e) {
        e.preventDefault();
        slot.classList.add("drag-over");
      });

      slot.addEventListener("dragleave", function () {
        slot.classList.remove("drag-over");
      });

      slot.addEventListener("drop", async function (e) {
        e.preventDefault();
        if (plannerBusy) return;

        slot.classList.remove("drag-over");

        const jobId = e.dataTransfer.getData("jobId");
        const date = slot.dataset.date;
        const workstationId = slot.dataset.workstationId;

        if (!jobId || !date || !workstationId) return;

        setPlannerBusyState(true);

        try {
          const result = await updatePlannerJob(jobId, date, workstationId);

          if (activeDraggedWrap && activeDraggedWrap.classList.contains("planner-job-wrap")) {
            applyLiveScheduledMove(result.job);
          } else {
            window.location.reload();
          }
        } catch (err) {
          alert(err.message || "Planner update failed");
        } finally {
          setPlannerBusyState(false);
        }
      });
    });
  }

  function bindTooltipEvents() {
    document.addEventListener("mouseover", function (e) {
      const target = e.target.closest(".planner-job-bar, .planner-unscheduled-card");
      if (!target) return;
      if (e.target.closest(".planner-split-popover")) return;
      if (activeTooltipTarget !== target) {
        showTooltip(target);
      }
    });

    document.addEventListener("mousemove", function () {
      if (activeTooltipTarget) {
        positionTooltip(activeTooltipTarget);
      }
    });

    document.addEventListener("mouseout", function (e) {
      const leaving = e.target.closest(".planner-job-bar, .planner-unscheduled-card");
      if (!leaving) return;

      const related = e.relatedTarget;
      if (related && leaving.contains(related)) return;

      hideTooltip();
    });

    window.addEventListener("scroll", function () {
      if (activeTooltipTarget) positionTooltip(activeTooltipTarget);
      if (activePopover && !activePopover.hidden) {
        const opener = document.querySelector(`[data-open-split="${activePopover.id}"]`);
        if (opener) positionPopover(activePopover, opener);
      }
    }, { passive: true });

    window.addEventListener("resize", function () {
      if (activeTooltipTarget) positionTooltip(activeTooltipTarget);
      if (activePopover && !activePopover.hidden) {
        const opener = document.querySelector(`[data-open-split="${activePopover.id}"]`);
        if (opener) positionPopover(activePopover, opener);
      }
      closeAllCustomSelects();
    });
  }

  function bindDocumentEvents() {
    document.addEventListener("click", function (e) {
      const openBtn = e.target.closest("[data-open-split]");
      if (openBtn) {
        e.preventDefault();
        e.stopPropagation();
        hideTooltip();
        openPopover(openBtn);
        return;
      }

      const closeBtn = e.target.closest("[data-close-split]");
      if (closeBtn) {
        e.preventDefault();
        e.stopPropagation();
        closeAllPopovers();
        return;
      }

      const addBtn = e.target.closest("[data-add-split-row]");
      if (addBtn) {
        e.preventDefault();

        const form = addBtn.closest("form");
        const rowsWrap = form.querySelector("[data-split-rows]");
        const sourceSelect = rowsWrap.querySelector("select");

        if (!sourceSelect) return;

        const cleanSelect = sourceSelect.cloneNode(true);
        cleanSelect.classList.remove("planner-native-select-hidden");
        cleanSelect.classList.add("planner-custom-select");
        cleanSelect.removeAttribute("data-enhanced");
        cleanSelect.style.cssText = "";

        const temp = document.createElement("div");
        temp.appendChild(cleanSelect);

        const newRow = buildSplitRow(temp.innerHTML);
        rowsWrap.appendChild(newRow);
        enhanceCustomSelects(newRow);

        if (activePopover && !activePopover.hidden) {
          const opener = document.querySelector(`[data-open-split="${activePopover.id}"]`);
          if (opener) positionPopover(activePopover, opener);
        }
        return;
      }

      const removeBtn = e.target.closest("[data-remove-split-row]");
      if (removeBtn) {
        e.preventDefault();
        const row = removeBtn.closest(".split-row");
        const wrap = row.parentElement;

        if (wrap.querySelectorAll(".split-row").length > 2) {
          row.remove();

          if (activePopover && !activePopover.hidden) {
            const opener = document.querySelector(`[data-open-split="${activePopover.id}"]`);
            if (opener) positionPopover(activePopover, opener);
          }
        }
        return;
      }

      const insidePopover = e.target.closest(".planner-split-popover");
      const insideSelect = e.target.closest(".planner-select");
      const scheduledWrap = e.target.closest(".planner-job-wrap");
      const unscheduledCard = e.target.closest(".planner-unscheduled-card");

      if (!insideSelect) {
        closeAllCustomSelects();
      }

      if (
        scheduledWrap &&
        !e.target.closest("button, a, input, select, textarea, .planner-split-popover") &&
        Date.now() > suppressClickUntil
      ) {
        if (scheduledWrap.dataset.jobsUrl) {
          window.location.href = scheduledWrap.dataset.jobsUrl;
          return;
        }
      }

      if (
        unscheduledCard &&
        !e.target.closest("button, a, input, select, textarea, .planner-split-popover") &&
        Date.now() > suppressClickUntil
      ) {
        if (unscheduledCard.dataset.jobsUrl) {
          window.location.href = unscheduledCard.dataset.jobsUrl;
          return;
        }
      }

      if (!insidePopover && !e.target.closest("[data-open-split]")) {
        closeAllPopovers();
      }
    });

    document.addEventListener("submit", async function (e) {
      const form = e.target.closest("[data-planner-split-form]");
      if (!form) return;

      e.preventDefault();

      try {
        await submitSplit(form);
      } catch (err) {
        alert(err.message || "Split failed");
      }
    });
  }

  function bindPlannerWheelPassthrough() {
    const plannerScroll = document.querySelector(".planner-scroll");
    if (!plannerScroll) return;

    plannerScroll.addEventListener("wheel", function (e) {
      const mostlyVertical = Math.abs(e.deltaY) > Math.abs(e.deltaX);

      if (mostlyVertical) {
        return;
      }

      if (e.deltaX !== 0) {
        e.preventDefault();
        plannerScroll.scrollLeft += e.deltaX;
        return;
      }

      if (e.shiftKey && e.deltaY !== 0) {
        e.preventDefault();
        plannerScroll.scrollLeft += e.deltaY;
      }
    }, { passive: false });
  }

  document.addEventListener("DOMContentLoaded", function () {
    enhanceCustomSelects(document);
    bindPlannerDrag();
    bindTooltipEvents();
    bindDocumentEvents();
    bindPlannerWheelPassthrough();
    bindPlannerDragAutoScrollTracking();
  });
})();