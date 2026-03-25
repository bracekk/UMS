(function () {
  const MOBILE_BREAKPOINT = 1024;

  function isMobileShell() {
    return window.innerWidth <= MOBILE_BREAKPOINT;
  }

  function initOrbitAnimation() {
    const section = document.querySelector(".capabilities-orbit-section");
    if (!section || typeof IntersectionObserver === "undefined") return;

    let played = false;
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting && !played) {
            played = true;
            section.classList.add("orbit-active");
            setTimeout(() => section.classList.remove("orbit-active"), 3200);
          }
        });
      },
      { threshold: 0.35 },
    );

    observer.observe(section);
  }

  function initAccountMenu() {
    const trigger = document.getElementById("accountMenuTrigger");
    const dropdown = document.getElementById("accountMenuDropdown");
    if (!trigger || !dropdown || trigger.dataset.bound === "1") return;

    trigger.dataset.bound = "1";

    trigger.addEventListener("click", function (e) {
      e.stopPropagation();
      dropdown.classList.toggle("open");
      trigger.classList.toggle("open");
    });

    document.addEventListener("click", function () {
      dropdown.classList.remove("open");
      trigger.classList.remove("open");
    });

    dropdown.addEventListener("click", function (e) {
      e.stopPropagation();
    });
  }

  function initMobileSidebar() {
    const app = document.getElementById("appShell");
    const sidebar = document.getElementById("workspaceSidebar");
    const overlay = document.getElementById("appShellOverlay");
    const openBtn = document.getElementById("mobileNavToggle");
    const closeBtn = document.getElementById("sidebarCloseBtn");

    if (!app || !sidebar || !overlay || !openBtn || openBtn.dataset.bound === "1") return;
    openBtn.dataset.bound = "1";

    function syncState() {
      if (!isMobileShell()) {
        app.classList.remove("sidebar-open");
        overlay.hidden = true;
        document.body.classList.remove("mobile-nav-open");
        openBtn.setAttribute("aria-expanded", "false");
        return;
      }

      const isOpen = app.classList.contains("sidebar-open");
      overlay.hidden = !isOpen;
      document.body.classList.toggle("mobile-nav-open", isOpen);
      openBtn.setAttribute("aria-expanded", isOpen ? "true" : "false");
    }

    function openSidebar() {
      if (!isMobileShell()) return;
      app.classList.add("sidebar-open");
      syncState();
    }

    function closeSidebar() {
      app.classList.remove("sidebar-open");
      syncState();
    }

    openBtn.addEventListener("click", function (e) {
      e.stopPropagation();
      if (app.classList.contains("sidebar-open")) {
        closeSidebar();
      } else {
        openSidebar();
      }
    });

    if (closeBtn) {
      closeBtn.addEventListener("click", function (e) {
        e.stopPropagation();
        closeSidebar();
      });
    }

    overlay.addEventListener("click", function () {
      closeSidebar();
    });

    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape") {
        closeSidebar();
      }
    });

    sidebar.querySelectorAll("a").forEach((link) => {
      link.addEventListener("click", function () {
        if (isMobileShell()) {
          closeSidebar();
        }
      });
    });

    window.addEventListener("resize", syncState, { passive: true });
    syncState();
  }

  function initStatusCheckboxes() {
    const statusInputs = Array.from(
      document.querySelectorAll('input[name="status"]'),
    );
    if (!statusInputs.length) return;

    const allInput = statusInputs.find((input) => input.value === "All");
    const otherInputs = statusInputs.filter((input) => input.value !== "All");

    function syncStatusCheckboxes(changedInput) {
      if (!allInput) return;
      if (changedInput === allInput && allInput.checked) {
        otherInputs.forEach((input) => {
          input.checked = false;
        });
        return;
      }
      if (changedInput !== allInput && changedInput.checked) {
        allInput.checked = false;
      }
      const anyOtherChecked = otherInputs.some((input) => input.checked);
      if (!anyOtherChecked) {
        allInput.checked = true;
      }
    }

    statusInputs.forEach((input) => {
      if (input.dataset.bound === "1") return;
      input.dataset.bound = "1";
      input.addEventListener("change", function () {
        syncStatusCheckboxes(this);
      });
    });

    if (allInput) {
      allInput.checked = !otherInputs.some((input) => input.checked);
    }
  }

  function initLiveFilters() {
    document.querySelectorAll(".live-filter-form").forEach((form) => {
      if (form.dataset.liveBound === "1") return;
      form.dataset.liveBound = "1";

      form.querySelectorAll("input, select, textarea").forEach((input) => {
        if (input.type === "text") return;
        input.addEventListener("change", () => form.submit());
      });

      form.querySelectorAll('input[type="text"]').forEach((input) => {
        input.addEventListener("keypress", function (e) {
          if (e.key === "Enter") {
            e.preventDefault();
            form.submit();
          }
        });
      });
    });
  }

  function initFlashMessages() {
    document.querySelectorAll(".flash-message").forEach((message) => {
      if (message.dataset.bound === "1") return;
      message.dataset.bound = "1";
      setTimeout(() => message.classList.add("flash-hide"), 2600);
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    initOrbitAnimation();
    initAccountMenu();
    initMobileSidebar();
    initStatusCheckboxes();
    initLiveFilters();
    initFlashMessages();
  });
})();