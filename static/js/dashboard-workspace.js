document.addEventListener("DOMContentLoaded", function () {
  const shell = document.getElementById("dashboardCanvasShell");
  const canvas = document.getElementById("dashboardCanvas");

  if (!shell || !canvas) return;

  const customizeBtn = document.getElementById("dashboardCustomizeBtn");
  const resetBtn = document.getElementById("dashboardResetBtn");
  const exportBtn = document.getElementById("dashboardExportBtn");
  const modePill = document.getElementById("dashboardLayoutModePill");
  const modeText = document.getElementById("dashboardLayoutModeText");

  const saveUrl = shell.dataset.saveUrl || "";
  const defaultLayout = JSON.parse(shell.dataset.defaultLayout || "[]");
  const widgets = Array.from(canvas.querySelectorAll(".dashboard-widget"));

  const storageKey = "ums-dashboard-layout-v9";
  const customizeKey = "ums-dashboard-customize-v9";

  const SNAP = 8;
  const MIN_W = 240;
  const MIN_H = 100;
  const CANVAS_BREAKPOINT = 1280;

  let customizeMode = localStorage.getItem(customizeKey) === "true";
  let interaction = null;
  let zCounter = 30;
  let resizeTicking = false;

  function isCanvasMode() {
    return window.innerWidth >= CANVAS_BREAKPOINT;
  }

  function snap(value) {
    return Math.round(value / SNAP) * SNAP;
  }

  function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
  }

  function getWidgetById(widgetId) {
    return canvas.querySelector(`.dashboard-widget[data-widget-id="${widgetId}"]`);
  }

  function getStoredFrame(widget) {
    return {
      x: parseFloat(widget.dataset.x || "0"),
      y: parseFloat(widget.dataset.y || "0"),
      w: parseFloat(widget.dataset.w || "320"),
      h: parseFloat(widget.dataset.h || "180"),
    };
  }

  function setStoredFrame(widget, frame) {
    widget.dataset.x = String(frame.x);
    widget.dataset.y = String(frame.y);
    widget.dataset.w = String(frame.w);
    widget.dataset.h = String(frame.h);
  }

  function normalizeFrame(frame) {
    const shellWidth = canvas.clientWidth || canvas.offsetWidth || 1400;
    const maxWidth = Math.max(MIN_W, shellWidth);

    const w = clamp(snap(frame.w), MIN_W, maxWidth);
    const h = clamp(snap(frame.h), MIN_H, 1600);
    const x = clamp(snap(frame.x), 0, Math.max(0, shellWidth - w));
    const y = Math.max(0, snap(frame.y));

    return { x, y, w, h };
  }

  function clearWidgetFrameStyles(widget) {
    widget.style.left = "";
    widget.style.top = "";
    widget.style.width = "";
    widget.style.height = "";
    widget.style.zIndex = "";
  }

  function updateAdaptiveClasses(widget) {
    const frame = getStoredFrame(widget);
    const card = widget.querySelector(".dashboard-widget-card");

    if (!card) return;

    card.classList.toggle("widget-xs", frame.w < 300);
    card.classList.toggle("widget-sm", frame.w >= 300 && frame.w < 420);
    card.classList.toggle("widget-md", frame.w >= 420 && frame.w < 700);
    card.classList.toggle("widget-lg", frame.w >= 700);

    card.classList.toggle("widget-short", frame.h < 150);
    card.classList.toggle("widget-mid", frame.h >= 150 && frame.h < 260);
    card.classList.toggle("widget-tall", frame.h >= 260);
  }

  function updateCanvasHeight() {
    if (!(customizeMode && isCanvasMode())) {
      canvas.style.height = "";
      return;
    }

    let maxBottom = 760;

    widgets.forEach((widget) => {
      const frame = getStoredFrame(widget);
      maxBottom = Math.max(maxBottom, frame.y + frame.h + 32);
    });

    canvas.style.height = `${maxBottom}px`;
  }

  function applyAbsoluteFrame(widget, frame, persist = true) {
    const normalized = normalizeFrame(frame);

    if (persist) {
      setStoredFrame(widget, normalized);
    }

    widget.style.left = `${normalized.x}px`;
    widget.style.top = `${normalized.y}px`;
    widget.style.width = `${normalized.w}px`;
    widget.style.height = `${normalized.h}px`;

    updateAdaptiveClasses(widget);
  }

  function serializeLayout() {
    return widgets.map((widget) => ({
      id: widget.dataset.widgetId,
      x: parseFloat(widget.dataset.x || "0"),
      y: parseFloat(widget.dataset.y || "0"),
      w: parseFloat(widget.dataset.w || "320"),
      h: parseFloat(widget.dataset.h || "180"),
    }));
  }

  function saveLocal() {
    localStorage.setItem(storageKey, JSON.stringify(serializeLayout()));
  }

  function saveServer() {
    if (!saveUrl) return;

    fetch(saveUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        layout: serializeLayout(),
      }),
    }).catch((error) => {
      console.warn("Dashboard save failed", error);
    });
  }

  function getInitialLayout() {
    try {
      const localLayout = JSON.parse(localStorage.getItem(storageKey));
      if (Array.isArray(localLayout) && localLayout.length) {
        return localLayout;
      }
    } catch (error) {
      console.warn("Local layout parse failed", error);
    }

    if (
      Array.isArray(window.UMS_DASHBOARD_LAYOUT) &&
      window.UMS_DASHBOARD_LAYOUT.length
    ) {
      return window.UMS_DASHBOARD_LAYOUT;
    }

    return defaultLayout;
  }

  function seedWidgetFramesFromLayout(layout) {
    layout.forEach((item) => {
      const widget = getWidgetById(item.id);
      if (!widget) return;
      setStoredFrame(widget, normalizeFrame(item));
    });

    widgets.forEach((widget) => {
      const hasFrame =
        typeof widget.dataset.x !== "undefined" &&
        typeof widget.dataset.y !== "undefined" &&
        typeof widget.dataset.w !== "undefined" &&
        typeof widget.dataset.h !== "undefined";

      if (!hasFrame) {
        setStoredFrame(
          widget,
          normalizeFrame({
            x: 0,
            y: 0,
            w: 320,
            h: 180,
          }),
        );
      }

      updateAdaptiveClasses(widget);
    });
  }

  function elevate(widget) {
    zCounter += 1;
    widget.style.zIndex = String(zCounter);
  }

  function applyOrganizedMode() {
    document.body.classList.add("dashboard-organized-mode");
    document.body.classList.remove("dashboard-customize-mode");

    widgets.forEach((widget) => {
      clearWidgetFrameStyles(widget);
      updateAdaptiveClasses(widget);
    });

    canvas.style.height = "";
  }

  function applyCustomizeMode() {
    document.body.classList.remove("dashboard-organized-mode");
    document.body.classList.add("dashboard-customize-mode");

    widgets.forEach((widget) => {
      const stored = getStoredFrame(widget);
      applyAbsoluteFrame(widget, stored, true);
    });

    updateCanvasHeight();
  }

  function updateModeUI() {
    const activeEditMode = customizeMode && isCanvasMode();

    if (modePill) {
      modePill.classList.toggle("editing", activeEditMode);
    }

    if (modeText) {
      if (activeEditMode) {
        modeText.textContent = "Layout editing";
      } else if (isCanvasMode()) {
        modeText.textContent = "Organized view";
      } else {
        modeText.textContent = "Responsive view";
      }
    }

    if (customizeBtn) {
      customizeBtn.disabled = !isCanvasMode();
      customizeBtn.classList.toggle("is-disabled", !isCanvasMode());
      customizeBtn.textContent = activeEditMode ? "Done" : "Customize Layout";
    }
  }

  function refreshLayoutMode() {
    if (customizeMode && isCanvasMode()) {
      applyCustomizeMode();
    } else {
      applyOrganizedMode();
    }

    updateModeUI();
  }

  function setCustomizeMode(enabled) {
    customizeMode = enabled && isCanvasMode();
    localStorage.setItem(customizeKey, String(customizeMode));
    refreshLayoutMode();
  }

  function resetToDefault() {
    seedWidgetFramesFromLayout(defaultLayout);
    localStorage.removeItem(storageKey);
    saveServer();

    setCustomizeMode(false);
  }

  function beginInteraction(type, widget, handle, startEvent) {
    if (!(customizeMode && isCanvasMode())) return;

    const startFrame = getStoredFrame(widget);
    const canvasRect = canvas.getBoundingClientRect();

    interaction = {
      type,
      widget,
      handle,
      startX: startEvent.clientX,
      startY: startEvent.clientY,
      startFrame,
      canvasRect,
    };

    elevate(widget);
    widget.classList.add("is-interacting");

    document.addEventListener("mousemove", onPointerMove);
    document.addEventListener("mouseup", endInteraction);
  }

  function onPointerMove(event) {
    if (!interaction) return;

    const dx = event.clientX - interaction.startX;
    const dy = event.clientY - interaction.startY;

    if (interaction.type === "drag") {
      applyAbsoluteFrame(
        interaction.widget,
        {
          x: interaction.startFrame.x + dx,
          y: interaction.startFrame.y + dy,
          w: interaction.startFrame.w,
          h: interaction.startFrame.h,
        },
        true,
      );
      updateCanvasHeight();
      return;
    }

    let nextFrame = { ...interaction.startFrame };

    if (interaction.handle === "x" || interaction.handle === "xy") {
      nextFrame.w = interaction.startFrame.w + dx;
    }

    if (interaction.handle === "y" || interaction.handle === "xy") {
      nextFrame.h = interaction.startFrame.h + dy;
    }

    applyAbsoluteFrame(interaction.widget, nextFrame, true);
    updateCanvasHeight();
  }

  function endInteraction() {
    if (!interaction) return;

    interaction.widget.classList.remove("is-interacting");
    interaction = null;

    saveLocal();
    saveServer();

    document.removeEventListener("mousemove", onPointerMove);
    document.removeEventListener("mouseup", endInteraction);
  }

  function bindInteractions() {
    widgets.forEach((widget) => {
      const dragHandle = widget.querySelector(".widget-drag-handle");
      const resizeHandles = widget.querySelectorAll(".widget-resize");

      if (dragHandle) {
        dragHandle.addEventListener("mousedown", function (event) {
          event.preventDefault();
          event.stopPropagation();
          beginInteraction("drag", widget, null, event);
        });
      }

      resizeHandles.forEach((handle) => {
        handle.addEventListener("mousedown", function (event) {
          event.preventDefault();
          event.stopPropagation();
          beginInteraction("resize", widget, handle.dataset.resize, event);
        });
      });
    });
  }

  function liveResizeRefresh() {
    if (resizeTicking) return;

    resizeTicking = true;

    requestAnimationFrame(() => {
      refreshLayoutMode();
      resizeTicking = false;
    });
  }

  if (customizeBtn) {
    customizeBtn.addEventListener("click", function () {
      setCustomizeMode(!customizeMode);
    });
  }

  if (resetBtn) {
    resetBtn.addEventListener("click", function () {
      resetToDefault();
    });
  }

  if (exportBtn) {
    exportBtn.addEventListener("click", function () {
      window.print();
    });
  }

  seedWidgetFramesFromLayout(getInitialLayout());
  bindInteractions();
  refreshLayoutMode();

  window.addEventListener("resize", liveResizeRefresh);
});