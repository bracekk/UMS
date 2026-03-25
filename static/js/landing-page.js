document.addEventListener("DOMContentLoaded", function () {
  const header = document.getElementById("landingHeader");
  const heroSection = document.querySelector(".hero-section");
  const revealItems = Array.from(document.querySelectorAll(".landing-reveal"));
  const allSections = Array.from(
    document.querySelectorAll(
      ".landing-section, .pricing-section, .workspace-preview-section, .contact-section",
    ),
  );
  const orbitSections = document.querySelectorAll(
    ".capabilities-orbit-section, .why-orbit-section",
  );
  const tiltCards = document.querySelectorAll(".tilt-card");
  const heroVisualPanel = document.getElementById("heroVisualPanel");
  const featuredPlan = document.querySelector(".pricing-card.is-featured");
  const prefersReducedMotion = window.matchMedia(
    "(prefers-reduced-motion: reduce)",
  ).matches;

  function updateHeaderState() {
    if (!header) return;
    header.classList.toggle("is-scrolled", window.scrollY > 8);
  }

  updateHeaderState();
  window.addEventListener("scroll", updateHeaderState, { passive: true });

  if (heroSection) {
    heroSection.querySelectorAll(".landing-reveal").forEach((item) => {
      item.classList.add("is-visible");
      item.classList.remove("reveal-pending");
    });
  }

  const nonHeroRevealItems = revealItems.filter(
    (item) => !item.closest(".hero-section"),
  );

  if (!prefersReducedMotion && nonHeroRevealItems.length) {
    const revealObserver = new IntersectionObserver(
      (entries, observer) => {
        entries.forEach((entry) => {
          if (!entry.isIntersecting) return;
          entry.target.classList.add("is-visible");
          entry.target.classList.remove("reveal-pending");
          observer.unobserve(entry.target);
        });
      },
      {
        threshold: 0.12,
        rootMargin: "0px 0px -8% 0px",
      },
    );

    nonHeroRevealItems.forEach((item) => {
      const rect = item.getBoundingClientRect();
      const inView = rect.top < window.innerHeight * 0.9;

      if (inView) {
        item.classList.add("is-visible");
        item.classList.remove("reveal-pending");
      } else {
        item.classList.add("reveal-pending");
        revealObserver.observe(item);
      }
    });
  } else {
    nonHeroRevealItems.forEach((item) => {
      item.classList.add("is-visible");
      item.classList.remove("reveal-pending");
    });
  }

  if (allSections.length) {
    const sectionObserver = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (!entry.isIntersecting) return;
          entry.target.classList.add("section-in-view");
        });
      },
      {
        threshold: 0.18,
        rootMargin: "0px 0px -10% 0px",
      },
    );

    allSections.forEach((section) => {
      sectionObserver.observe(section);
    });
  }

  if (!prefersReducedMotion && orbitSections.length) {
    const orbitObserver = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (!entry.isIntersecting) return;

          const section = entry.target;
          section.classList.add("orbit-active");

          window.setTimeout(() => {
            section.classList.remove("orbit-active");
          }, 1400);
        });
      },
      {
        threshold: 0.28,
      },
    );

    orbitSections.forEach((section) => orbitObserver.observe(section));
  }

  if (!prefersReducedMotion && featuredPlan) {
    let featuredAnimated = false;

    const featuredObserver = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (!entry.isIntersecting) return;
          if (featuredAnimated) return;

          featuredAnimated = true;
          featuredPlan.classList.add("plan-animate");

          window.setTimeout(() => {
            featuredPlan.classList.remove("plan-animate");
          }, 2800);
        });
      },
      {
        threshold: 0.35,
      },
    );

    featuredObserver.observe(featuredPlan);
  }

  if (!prefersReducedMotion && typeof gsap !== "undefined") {
    const heroTimeline = gsap.timeline({
      defaults: {
        ease: "power3.out",
      },
    });

    heroTimeline
      .from(".hero-badge-row", {
        y: 18,
        opacity: 0,
        duration: 0.42,
      })
      .from(
        ".hero-copy h1",
        {
          y: 28,
          opacity: 0,
          duration: 0.66,
        },
        "-=0.16",
      )
      .from(
        ".hero-text",
        {
          y: 16,
          opacity: 0,
          duration: 0.5,
        },
        "-=0.34",
      )
      .from(
        ".hero-actions .landing-btn",
        {
          y: 12,
          opacity: 0,
          stagger: 0.08,
          duration: 0.34,
        },
        "-=0.24",
      )
      .from(
        ".hero-proof-pill",
        {
          y: 10,
          opacity: 0,
          stagger: 0.05,
          duration: 0.28,
        },
        "-=0.18",
      )
      .from(
        ".hero-metric",
        {
          y: 10,
          opacity: 0,
          stagger: 0.06,
          duration: 0.3,
        },
        "-=0.16",
      )
      .from(
        ".hero-visual-panel",
        {
          y: 20,
          opacity: 0,
          scale: 0.988,
          duration: 0.56,
        },
        "-=0.42",
      )
      .from(
        ".hero-floating-chip",
        {
          y: 8,
          opacity: 0,
          stagger: 0.04,
          duration: 0.24,
        },
        "-=0.14",
      );

    gsap.to(".landing-ambient-a", {
      x: 14,
      y: -10,
      duration: 7.2,
      repeat: -1,
      yoyo: true,
      ease: "sine.inOut",
    });

    gsap.to(".landing-ambient-b", {
      x: -16,
      y: 14,
      duration: 8.8,
      repeat: -1,
      yoyo: true,
      ease: "sine.inOut",
    });

    if (heroVisualPanel) {
      gsap.to(heroVisualPanel, {
        y: -5,
        duration: 4.2,
        repeat: -1,
        yoyo: true,
        ease: "sine.inOut",
      });
    }
  }

  if (!prefersReducedMotion && tiltCards.length) {
    tiltCards.forEach((card) => {
      const intensity = card.classList.contains("hero-visual-panel") ? 6 : 4;

      card.addEventListener("mousemove", function (event) {
        const rect = card.getBoundingClientRect();
        const x = event.clientX - rect.left;
        const y = event.clientY - rect.top;
        const px = x / rect.width;
        const py = y / rect.height;
        const rotateY = (px - 0.5) * intensity;
        const rotateX = (0.5 - py) * intensity;

        card.style.transform = `perspective(1200px) rotateX(${rotateX}deg) rotateY(${rotateY}deg) translate3d(0, -1px, 0)`;
      });

      card.addEventListener("mouseleave", function () {
        card.style.transform = "";
      });
    });
  }

  document.querySelectorAll('a[href^="#"]').forEach((anchor) => {
    anchor.addEventListener("click", function (event) {
      const href = anchor.getAttribute("href");
      if (!href || href === "#") return;

      const target = document.querySelector(href);
      if (!target) return;

      event.preventDefault();

      const top = target.getBoundingClientRect().top + window.scrollY - 82;

      window.scrollTo({
        top,
        behavior: "smooth",
      });
    });
  });
});
