(() => {
  "use strict";

  const book = window.CODEBASE_BOOK;
  if (!book?.chapters?.length) {
    document.body.innerHTML = "<p>Codebase book content is missing. Run <code>node build-site.mjs</code>.</p>";
    return;
  }

  const view = document.getElementById("view");
  const chapterNav = document.getElementById("chapter-nav");
  const workflowNav = document.getElementById("workflow-nav");
  const breadcrumbs = document.getElementById("breadcrumbs");
  const progress = document.getElementById("reading-progress");
  const searchDialog = document.getElementById("search-dialog");
  const searchInput = document.getElementById("search-input");
  const searchHint = document.getElementById("search-hint");
  const searchResults = document.getElementById("search-results");

  const bySlug = new Map(book.chapters.map((chapter) => [chapter.slug, chapter]));
  const workflowBySlug = new Map((book.workflows || []).map((workflow) => [workflow.slug, workflow]));

  function escapeHtml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
  }

  function chapterRoute(chapter, heading) {
    return `#/chapter/${chapter.slug}${heading ? `/${heading.id}` : ""}`;
  }

  function workflowRoute(workflow) {
    return `#/flow/${workflow.slug}`;
  }

  function chapterNumber(chapter) {
    return String(chapter.order).padStart(2, "0");
  }

  function buildNavigation() {
    workflowNav.innerHTML = (book.workflows || [])
      .map(
        (workflow, index) => `
          <a class="workflow-link" href="${workflowRoute(workflow)}" data-workflow="${workflow.slug}">
            <span class="nav-number">F${index + 1}</span>
            <span>${escapeHtml(workflow.shortTitle)}</span>
          </a>`,
      )
      .join("");

    chapterNav.innerHTML = book.chapters
      .map(
        (chapter) => `
          <a class="chapter-link" href="${chapterRoute(chapter)}" data-chapter="${chapter.slug}">
            <span class="nav-number">${chapterNumber(chapter)}</span>
            <span>${escapeHtml(chapter.title)}</span>
          </a>`,
      )
      .join("");
  }

  function parseRoute() {
    const raw = location.hash.replace(/^#\/?/, "");
    if (!raw) return { type: "home" };
    const parts = raw.split("/").filter(Boolean);
    if (parts[0] === "flow" && parts[1]) {
      return { type: "flow", slug: parts[1] };
    }
    if (parts[0] !== "chapter" || !parts[1]) return { type: "home" };
    return {
      type: "chapter",
      slug: parts[1],
      section: parts.slice(2).join("/") || null,
    };
  }

  function setActiveNavigation(chapterSlug, workflowSlug) {
    document.querySelector(".nav-home")?.classList.toggle("active", !chapterSlug && !workflowSlug);
    document.querySelectorAll(".chapter-link").forEach((link) => {
      link.classList.toggle("active", link.dataset.chapter === chapterSlug);
    });
    document.querySelectorAll(".workflow-link").forEach((link) => {
      link.classList.toggle("active", link.dataset.workflow === workflowSlug);
    });
  }

  function setBreadcrumb(current) {
    breadcrumbs.innerHTML = `
      <span>Codebase Book</span>
      <span>/</span>
      <strong>${escapeHtml(current)}</strong>`;
  }

  function renderHome() {
    setActiveNavigation(null, null);
    setBreadcrumb("Overview");
    document.title = "Messaging Provider · Codebase Book";

    const cards = book.chapters
      .map((chapter) => {
        const firstSection = chapter.headings.find((heading) => heading.level === 2);
        return `
          <a class="chapter-card" href="${chapterRoute(chapter)}">
            <span class="chapter-number">Chapter ${chapterNumber(chapter)}</span>
            <h3>${escapeHtml(chapter.title)}</h3>
            <p>${firstSection ? `Starts with ${escapeHtml(firstSection.title)}.` : `${chapter.lineCount} source-backed lines.`}</p>
          </a>`;
      })
      .join("");
    const workflowCards = (book.workflows || [])
      .map(
        (workflow, index) => `
          <a class="workflow-card" href="${workflowRoute(workflow)}">
            <span class="workflow-index">Flow ${String(index + 1).padStart(2, "0")}</span>
            <h3>${escapeHtml(workflow.title)}</h3>
            <p>${escapeHtml(workflow.summary)}</p>
            <span class="workflow-card-link">Open step-by-step example →</span>
          </a>`,
      )
      .join("");

    view.innerHTML = `
      <div class="home">
        <section class="hero">
          <p class="eyebrow">Architecture · Operations · Debugging</p>
          <h1>Understand the messaging provider before changing it.</h1>
          <p class="hero-copy">
            A source-backed guide to broker behavior, storage, APIs, POS state, delivery,
            compaction, retries, runtime configuration, and test coverage.
          </p>
          <div class="hero-actions">
            <a class="button primary" href="#/chapter/01-system-overview">Start with the system overview</a>
            <a class="button" href="#/chapter/12-debugging-guide">Open debugging guide</a>
          </div>
        </section>

        <section class="stat-grid" aria-label="Book statistics">
          <div class="stat-card"><strong>${book.stats.chapters}</strong><span>Chapters</span></div>
          <div class="stat-card"><strong>${book.stats.features}</strong><span>Feature sections</span></div>
          <div class="stat-card"><strong>${book.stats.apiSections}</strong><span>API groups</span></div>
          <div class="stat-card"><strong>${book.stats.lines.toLocaleString()}</strong><span>Source lines</span></div>
        </section>

        <div class="section-heading">
          <div>
            <p class="eyebrow">System at a glance</p>
            <h2>Primary data path</h2>
          </div>
          <p>Follow each box into the linked chapters below.</p>
        </div>
        <section class="architecture-flow" aria-label="Primary message flow">
          <div class="flow-node"><strong>Producer / parent</strong><span>TCP DATA or HTTP pipe records</span></div>
          <div class="flow-arrow">→</div>
          <div class="flow-node"><strong>Broker ingest</strong><span>Validate, append, index</span></div>
          <div class="flow-arrow">→</div>
          <div class="flow-node"><strong>Segment storage</strong><span>Log, index, SQLite metadata</span></div>
          <div class="flow-arrow">→</div>
          <div class="flow-node"><strong>POS consumer</strong><span>BATCH_HEADER, bytes, BATCH_ACK</span></div>
        </section>

        <div class="section-heading">
          <div>
            <p class="eyebrow">How the system works</p>
            <h2>Step-by-step workflow examples</h2>
          </div>
          <p>Concrete scenarios linked back to source files and detailed chapters.</p>
        </div>
        <section class="workflow-grid">${workflowCards}</section>

        <div class="section-heading">
          <div>
            <p class="eyebrow">Worked entry points</p>
            <h2>Choose the task you are doing</h2>
          </div>
        </div>
        <section class="task-grid">
          <a class="task-card" href="#/chapter/03-feature-catalog/producer-ingestion">
            <span class="task-kicker">Feature example</span>
            <h3>Trace a message from producer to ACK</h3>
            <p>Start at DataHandler, follow segment persistence, delivery filtering, and committed offsets.</p>
          </a>
          <a class="task-card" href="#/chapter/12-debugging-guide/consumer-receives-no-data">
            <span class="task-kicker">Debug example</span>
            <h3>Consumer receives no data</h3>
            <p>Check registration, readiness, refresh gates, storage head, pending ACK, and delivery backoff.</p>
          </a>
          <a class="task-card" href="#/chapter/07-pos-machine-state/refresh-state-machine">
            <span class="task-kicker">State example</span>
            <h3>Understand POS refresh</h3>
            <p>Walk RESET_SENT → REPLAYING → READY_SENT → COMPLETED and inspect persisted recovery.</p>
          </a>
          <a class="task-card" href="#/chapter/04-api-catalog">
            <span class="task-kicker">API example</span>
            <h3>Find an HTTP or TCP entry point</h3>
            <p>Map endpoints and protocol messages to controllers, handlers, data, and operational risks.</p>
          </a>
          <a class="task-card" href="#/chapter/08-compaction-handling">
            <span class="task-kicker">Safety example</span>
            <h3>Change compaction safely</h3>
            <p>Review delivery-time filtering, physical rewrites, checkpoints, concurrency, and POS impact.</p>
          </a>
          <a class="task-card" href="#/chapter/10-test-map">
            <span class="task-kicker">Verification example</span>
            <h3>Select the correct test suite</h3>
            <p>See unit, integration, journey, system, build, and static-analysis coverage and known failures.</p>
          </a>
        </section>

        <div class="section-heading">
          <div>
            <p class="eyebrow">Complete book</p>
            <h2>All chapters</h2>
          </div>
          <p>Use <kbd>/</kbd> to search every chapter.</p>
        </div>
        <section class="chapter-grid">${cards}</section>
      </div>`;

    window.scrollTo({ top: 0 });
  }

  function pagerLink(chapter, direction) {
    if (!chapter) return "<span></span>";
    return `
      <a class="pager-link" href="${chapterRoute(chapter)}">
        <span>${direction}</span>
        <strong>${escapeHtml(chapter.title)}</strong>
      </a>`;
  }

  function renderChapter(route) {
    const chapter = bySlug.get(route.slug);
    if (!chapter) {
      location.hash = "#/";
      return;
    }

    setActiveNavigation(chapter.slug, null);
    setBreadcrumb(chapter.title);
    document.title = `${chapter.title} · Codebase Book`;

    const toc = chapter.headings
      .filter((heading) => heading.level <= 3)
      .map(
        (heading) => `
          <a class="toc-link level-${heading.level}" href="${chapterRoute(chapter, heading)}" data-section="${heading.id}">
            ${escapeHtml(heading.title)}
          </a>`,
      )
      .join("");

    const previous = book.chapters[chapter.order - 1];
    const next = book.chapters[chapter.order + 1];

    view.innerHTML = `
      <div class="article-layout">
        <article class="article">
          <div class="article-meta">
            <span>Chapter ${chapterNumber(chapter)}</span>
            <span>${chapter.lineCount} Markdown lines</span>
            <span>${chapter.headings.length} sections</span>
          </div>
          <div class="markdown-body">${chapter.html}</div>
          <nav class="chapter-pager" aria-label="Adjacent chapters">
            ${pagerLink(previous, "Previous")}
            ${pagerLink(next, "Next")}
          </nav>
        </article>
        <aside class="article-toc">
          <strong>On this page</strong>
          ${toc || '<span class="toc-link">No subsections</span>'}
        </aside>
      </div>`;

    bindCopyButtons();
    observeHeadings();

    requestAnimationFrame(() => {
      if (route.section) {
        document.getElementById(route.section)?.scrollIntoView({ block: "start" });
      } else {
        window.scrollTo({ top: 0 });
      }
    });
  }

  function renderWorkflow(route) {
    const workflow = workflowBySlug.get(route.slug);
    if (!workflow) {
      location.hash = "#/";
      return;
    }

    setActiveNavigation(null, workflow.slug);
    setBreadcrumb(workflow.title);
    document.title = `${workflow.title} · Codebase Book`;

    const steps = workflow.steps
      .map(
        (step, index) => `
          <li class="workflow-step">
            <span class="step-number">${String(index + 1).padStart(2, "0")}</span>
            <div class="step-content">
              <div class="step-heading">
                <h3>${escapeHtml(step.title)}</h3>
                <code>${escapeHtml(step.signal)}</code>
              </div>
              <p>${escapeHtml(step.detail)}</p>
            </div>
          </li>`,
      )
      .join("");

    const safeguards = workflow.safeguards
      .map((item) => `<li>${escapeHtml(item)}</li>`)
      .join("");
    const sources = workflow.sources
      .map((source) => `<li><code>${escapeHtml(source)}</code></li>`)
      .join("");
    const related = workflow.related
      .map((item) => `<a class="button" href="${item.route}">${escapeHtml(item.label)}</a>`)
      .join("");

    view.innerHTML = `
      <div class="workflow-page">
        <header class="workflow-hero">
          <p class="eyebrow">${escapeHtml(workflow.category)}</p>
          <h1>${escapeHtml(workflow.title)}</h1>
          <p>${escapeHtml(workflow.summary)}</p>
          <div class="workflow-scenario">
            <span>Example scenario</span>
            <strong>${escapeHtml(workflow.scenario)}</strong>
          </div>
        </header>

        <div class="workflow-layout">
          <section>
            <div class="section-heading workflow-section-heading">
              <div>
                <p class="eyebrow">Execution path</p>
                <h2>How it works</h2>
              </div>
              <p>${workflow.steps.length} ordered steps</p>
            </div>
            <ol class="workflow-timeline">${steps}</ol>
          </section>

          <aside class="workflow-aside">
            <section class="outcome-card">
              <span>Result</span>
              <p>${escapeHtml(workflow.outcome)}</p>
            </section>
            <section class="detail-card">
              <h2>Safety and behavior</h2>
              <ul>${safeguards}</ul>
            </section>
            <section class="detail-card source-card">
              <h2>Primary sources</h2>
              <ul>${sources}</ul>
            </section>
          </aside>
        </div>

        <section class="workflow-related">
          <div>
            <p class="eyebrow">Go deeper</p>
            <h2>Detailed documentation</h2>
          </div>
          <div class="hero-actions">${related}</div>
        </section>
      </div>`;

    window.scrollTo({ top: 0 });
  }

  function bindCopyButtons() {
    document.querySelectorAll(".copy-code").forEach((button) => {
      button.addEventListener("click", async () => {
        const code = button.closest(".code-block")?.querySelector("code")?.textContent || "";
        let copied = false;
        if (navigator.clipboard?.writeText) {
          try {
            await navigator.clipboard.writeText(code);
            copied = true;
          } catch {
            copied = false;
          }
        }
        if (!copied) {
          const selection = window.getSelection();
          const range = document.createRange();
          range.selectNodeContents(button.closest(".code-block").querySelector("code"));
          selection.removeAllRanges();
          selection.addRange(range);
          copied = document.execCommand("copy");
          selection.removeAllRanges();
        }
        button.textContent = copied ? "Copied" : "Select text";
        setTimeout(() => {
          button.textContent = "Copy";
        }, 1200);
      });
    });
  }

  function observeHeadings() {
    const headings = [...document.querySelectorAll(".markdown-body [data-heading]")].filter(
      (heading) => heading.tagName !== "H1",
    );
    const tocLinks = [...document.querySelectorAll(".toc-link[data-section]")];
    if (!headings.length || !tocLinks.length) return;

    const observer = new IntersectionObserver(
      (entries) => {
        const visible = entries
          .filter((entry) => entry.isIntersecting)
          .sort((a, b) => a.boundingClientRect.top - b.boundingClientRect.top)[0];
        if (!visible) return;
        tocLinks.forEach((link) => {
          link.classList.toggle("active", link.dataset.section === visible.target.id);
        });
      },
      { rootMargin: "-90px 0px -72% 0px", threshold: [0, 1] },
    );
    headings.forEach((heading) => observer.observe(heading));
  }

  function renderRoute() {
    document.body.classList.remove("menu-open");
    const route = parseRoute();
    if (route.type === "chapter") renderChapter(route);
    else if (route.type === "flow") renderWorkflow(route);
    else renderHome();
    updateProgress();
  }

  function highlight(value, query) {
    const escaped = escapeHtml(value);
    const safeQuery = query.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    return escaped.replace(new RegExp(`(${safeQuery})`, "ig"), "<mark>$1</mark>");
  }

  function snippet(text, query) {
    const normalized = text.replace(/\s+/g, " ");
    const position = normalized.toLowerCase().indexOf(query.toLowerCase());
    const start = Math.max(0, position - 68);
    const end = Math.min(normalized.length, Math.max(position + query.length + 92, 180));
    return `${start > 0 ? "…" : ""}${normalized.slice(start, end)}${end < normalized.length ? "…" : ""}`;
  }

  function runSearch(query) {
    const normalized = query.trim().toLowerCase();
    if (normalized.length < 2) {
      searchHint.hidden = false;
      searchResults.innerHTML = "";
      return;
    }

    searchHint.hidden = true;
    const results = [];

    (book.workflows || []).forEach((workflow) => {
      const titleMatch = workflow.title.toLowerCase().includes(normalized);
      const contentMatch = workflow.searchText.toLowerCase().includes(normalized);
      if (titleMatch || contentMatch) {
        results.push({
          score: titleMatch ? 95 : 55,
          workflow,
          title: workflow.title,
          detail: contentMatch ? snippet(workflow.searchText, normalized) : workflow.category,
        });
      }
    });

    book.chapters.forEach((chapter) => {
      chapter.headings.forEach((heading) => {
        const score =
          heading.title.toLowerCase() === normalized
            ? 100
            : heading.title.toLowerCase().includes(normalized)
              ? 70
              : 0;
        if (score) {
          results.push({
            score,
            chapter,
            heading,
            title: heading.title,
            detail: chapter.title,
          });
        }
      });

      const titleMatch = chapter.title.toLowerCase().includes(normalized);
      const contentMatch = chapter.plainText.toLowerCase().includes(normalized);
      if (titleMatch || contentMatch) {
        results.push({
          score: titleMatch ? 80 : 30,
          chapter,
          title: chapter.title,
          detail: contentMatch ? snippet(chapter.plainText, normalized) : `${chapter.lineCount} source-backed lines`,
        });
      }
    });

    const unique = [];
    const seen = new Set();
    results
      .sort((a, b) => b.score - a.score || (a.chapter?.order ?? -1) - (b.chapter?.order ?? -1))
      .forEach((result) => {
        const key = result.workflow
          ? `workflow:${result.workflow.slug}`
          : `${result.chapter.slug}:${result.heading?.id || "chapter"}`;
        if (!seen.has(key) && unique.length < 18) {
          seen.add(key);
          unique.push(result);
        }
      });

    searchResults.innerHTML = unique.length
      ? unique
          .map(
            (result) => `
              <a class="search-result" href="${result.workflow ? workflowRoute(result.workflow) : chapterRoute(result.chapter, result.heading)}">
                <span class="result-number">${result.workflow ? "FLOW" : chapterNumber(result.chapter)}</span>
                <span>
                  <strong>${highlight(result.title, query)}</strong>
                  <span>${highlight(result.detail, query)}</span>
                </span>
              </a>`,
          )
          .join("")
      : '<div class="empty-state">No matching feature, API, class, state, or operational note.</div>';
  }

  function openSearch() {
    if (!searchDialog.open) searchDialog.showModal();
    searchInput.focus();
  }

  function closeSearch() {
    if (searchDialog.open) searchDialog.close();
  }

  function updateProgress() {
    const scrollable = document.documentElement.scrollHeight - window.innerHeight;
    const percent = scrollable > 0 ? Math.min(100, (window.scrollY / scrollable) * 100) : 0;
    progress.style.width = `${percent}%`;
  }

  buildNavigation();
  renderRoute();

  window.addEventListener("hashchange", () => {
    closeSearch();
    renderRoute();
  });
  window.addEventListener("scroll", updateProgress, { passive: true });

  document.getElementById("search-trigger").addEventListener("click", openSearch);
  searchInput.addEventListener("input", (event) => runSearch(event.target.value));
  searchDialog.addEventListener("click", (event) => {
    if (event.target === searchDialog) closeSearch();
  });

  document.addEventListener("keydown", (event) => {
    const typing = /INPUT|TEXTAREA/.test(document.activeElement?.tagName);
    if (event.key === "/" && !typing) {
      event.preventDefault();
      openSearch();
    }
    if (event.key === "Escape") closeSearch();
  });

  document.getElementById("theme-button").addEventListener("click", () => {
    const next = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
    document.documentElement.dataset.theme = next;
    localStorage.setItem("codebase-book-theme", next);
  });

  document.getElementById("menu-button").addEventListener("click", () => {
    document.body.classList.toggle("menu-open");
  });
  document.getElementById("mobile-backdrop").addEventListener("click", () => {
    document.body.classList.remove("menu-open");
  });
})();
