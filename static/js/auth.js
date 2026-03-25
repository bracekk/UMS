document.addEventListener("DOMContentLoaded", () => {
  const shell = document.querySelector(".auth-shell");
  const fields = document.querySelectorAll(".auth-field");
  const inputs = document.querySelectorAll(".auth-field input");
  const form = document.querySelector(".auth-form");

  if (shell) {
    shell.classList.add("auth-shell-ready");
  }

  fields.forEach((field, index) => {
    field.style.setProperty("--auth-delay", `${0.08 + index * 0.05}s`);
    field.classList.add("auth-field-reveal");
  });

  inputs.forEach((input) => {
    const field = input.closest(".auth-field");
    if (!field) return;

    const syncState = () => {
      if (input.value.trim()) {
        field.classList.add("has-value");
      } else {
        field.classList.remove("has-value");
      }
    };

    input.addEventListener("focus", () => field.classList.add("is-focused"));
    input.addEventListener("blur", () => field.classList.remove("is-focused"));
    input.addEventListener("input", syncState);

    syncState();

    if (input.type === "password") {
      const toggle = document.createElement("button");
      toggle.type = "button";
      toggle.className = "auth-password-toggle";
      toggle.setAttribute("aria-label", "Toggle password visibility");
      toggle.innerHTML = `
        <span class="auth-password-toggle-text">Show</span>
      `;

      toggle.addEventListener("click", () => {
        const isPassword = input.type === "password";
        input.type = isPassword ? "text" : "password";
        toggle.querySelector(".auth-password-toggle-text").textContent =
          isPassword ? "Hide" : "Show";
        field.classList.toggle("is-password-visible", isPassword);
      });

      field.classList.add("auth-field-password");
      field.appendChild(toggle);
    }
  });

  if (form) {
    form.addEventListener("submit", () => {
      const submitBtn = form.querySelector('button[type="submit"]');
      if (submitBtn) {
        submitBtn.classList.add("is-loading");
        submitBtn.disabled = true;
        const originalText = submitBtn.textContent;
        submitBtn.dataset.originalText = originalText;
        submitBtn.textContent = "Please wait...";
      }
    });
  }

  const cards = document.querySelectorAll(".auth-shell");
  cards.forEach((card) => {
    card.addEventListener("mousemove", (e) => {
      const rect = card.getBoundingClientRect();
      const x = ((e.clientX - rect.left) / rect.width) * 100;
      const y = ((e.clientY - rect.top) / rect.height) * 100;
      card.style.setProperty("--mx", `${x}%`);
      card.style.setProperty("--my", `${y}%`);
    });
  });
});
