import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const bookDir = path.dirname(fileURLToPath(import.meta.url));
const siteDir = path.join(bookDir, "site");
const assetsDir = path.join(siteDir, "assets");

const chapterFiles = fs
  .readdirSync(bookDir)
  .filter((name) => /^\d{2}-.*\.md$/.test(name))
  .sort();

if (chapterFiles.length === 0) {
  throw new Error(`No Markdown chapters found in ${bookDir}`);
}

fs.mkdirSync(assetsDir, { recursive: true });

const sourceByFile = new Map(
  chapterFiles.map((file) => [file, fs.readFileSync(path.join(bookDir, file), "utf8")]),
);
const knownChapterSlugs = new Set(chapterFiles.map((file) => file.replace(/\.md$/, "")));

function escapeHtml(value) {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function stripMarkdown(value) {
  return value
    .replace(/`([^`]+)`/g, "$1")
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
    .replace(/[*_~]/g, "")
    .trim();
}

function slugify(value) {
  return stripMarkdown(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9\s-]/g, "")
    .trim()
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-");
}

function inlineMarkdown(value, sourceFile) {
  const codeTokens = [];
  let rendered = value.replace(/`([^`]+)`/g, (_, code) => {
    const token = `\u0000CODE${codeTokens.length}\u0000`;
    codeTokens.push(`<code>${escapeHtml(code)}</code>`);
    return token;
  });

  rendered = escapeHtml(rendered);
  rendered = rendered.replace(
    /\[([^\]]+)\]\(([^)]+)\)/g,
    (_, label, rawHref) => {
      const href = rawHref.trim();
      const bookLink = href.match(/^(\d{2}-[^#]+)\.md(?:#(.+))?$/);
      if (bookLink) {
        const chapterSlug = bookLink[1];
        if (!knownChapterSlugs.has(chapterSlug)) {
          throw new Error(`${sourceFile}: link points to missing chapter ${href}`);
        }
        const section = bookLink[2] ? `/${bookLink[2]}` : "";
        return `<a href="#/chapter/${chapterSlug}${section}">${label}</a>`;
      }

      const external = /^https?:\/\//.test(href);
      if (/^readme\/.+\.md(?:#.+)?$/.test(href)) {
        return `<a href="../../../${escapeHtml(href)}">${label}</a>`;
      }
      const attributes = external ? ' target="_blank" rel="noreferrer"' : "";
      return `<a href="${escapeHtml(href)}"${attributes}>${label}</a>`;
    },
  );
  rendered = rendered
    .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
    .replace(/__([^_]+)__/g, "<strong>$1</strong>")
    .replace(/(?<!\*)\*([^*]+)\*(?!\*)/g, "<em>$1</em>")
    .replace(/(?<!_)_([^_]+)_(?!_)/g, "<em>$1</em>");

  codeTokens.forEach((token, index) => {
    rendered = rendered.replace(`\u0000CODE${index}\u0000`, token);
  });
  return rendered;
}

function parseTableRow(line) {
  const trimmed = line.trim().replace(/^\|/, "").replace(/\|$/, "");
  return trimmed.split("|").map((cell) => cell.trim());
}

function isTableDivider(line) {
  return /^\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|?\s*$/.test(line);
}

function listItem(line) {
  const match = line.match(/^(\s*)([-+*]|\d+\.)\s+(.+)$/);
  if (!match) return null;
  return {
    indent: match[1].replaceAll("\t", "  ").length,
    type: /\d+\./.test(match[2]) ? "ol" : "ul",
    text: match[3],
  };
}

function renderList(items, sourceFile) {
  let cursor = 0;

  function renderLevel(indent) {
    const type = items[cursor].type;
    let html = `<${type}>`;

    while (
      cursor < items.length &&
      items[cursor].indent === indent &&
      items[cursor].type === type
    ) {
      const item = items[cursor];
      cursor += 1;
      html += `<li>${inlineMarkdown(item.text, sourceFile)}`;

      while (cursor < items.length && items[cursor].indent > indent) {
        html += renderLevel(items[cursor].indent);
      }
      html += "</li>";
    }

    html += `</${type}>`;
    return html;
  }

  let html = "";
  while (cursor < items.length) {
    html += renderLevel(items[cursor].indent);
  }
  return html;
}

function renderMarkdown(markdown, sourceFile) {
  const lines = markdown.replace(/\r\n/g, "\n").split("\n");
  const headingCounts = new Map();
  const headings = [];
  const html = [];
  let index = 0;

  while (index < lines.length) {
    const line = lines[index];

    if (!line.trim()) {
      index += 1;
      continue;
    }

    const fence = line.match(/^```(\w*)\s*$/);
    if (fence) {
      const language = fence[1] || "text";
      const code = [];
      index += 1;
      while (index < lines.length && !/^```\s*$/.test(lines[index])) {
        code.push(lines[index]);
        index += 1;
      }
      index += 1;
      html.push(
        `<div class="code-block"><div class="code-toolbar"><span>${escapeHtml(language)}</span><button class="copy-code" type="button">Copy</button></div><pre><code>${escapeHtml(code.join("\n"))}</code></pre></div>`,
      );
      continue;
    }

    const heading = line.match(/^(#{1,4})\s+(.+)$/);
    if (heading) {
      const level = heading[1].length;
      const title = stripMarkdown(heading[2]);
      const baseId = slugify(title) || `section-${index}`;
      const seen = headingCounts.get(baseId) || 0;
      headingCounts.set(baseId, seen + 1);
      const id = seen === 0 ? baseId : `${baseId}-${seen}`;
      headings.push({ level, title, id });
      html.push(
        `<h${level} id="${id}" data-heading="${escapeHtml(title)}">${inlineMarkdown(heading[2], sourceFile)}<a class="heading-link" href="#/chapter/${sourceFile.replace(/\.md$/, "")}/${id}" aria-label="Link to ${escapeHtml(title)}">#</a></h${level}>`,
      );
      index += 1;
      continue;
    }

    if (
      line.trim().startsWith("|") &&
      index + 1 < lines.length &&
      isTableDivider(lines[index + 1])
    ) {
      const headers = parseTableRow(line);
      const rows = [];
      index += 2;
      while (index < lines.length && lines[index].trim().startsWith("|")) {
        rows.push(parseTableRow(lines[index]));
        index += 1;
      }
      html.push('<div class="table-wrap"><table><thead><tr>');
      headers.forEach((cell) => {
        html.push(`<th>${inlineMarkdown(cell, sourceFile)}</th>`);
      });
      html.push("</tr></thead><tbody>");
      rows.forEach((row) => {
        html.push("<tr>");
        headers.forEach((_, cellIndex) => {
          html.push(`<td>${inlineMarkdown(row[cellIndex] || "", sourceFile)}</td>`);
        });
        html.push("</tr>");
      });
      html.push("</tbody></table></div>");
      continue;
    }

    if (listItem(line)) {
      const items = [];
      while (index < lines.length) {
        const item = listItem(lines[index]);
        if (!item) break;
        items.push(item);
        index += 1;
      }
      html.push(renderList(items, sourceFile));
      continue;
    }

    if (/^>\s?/.test(line)) {
      const quote = [];
      while (index < lines.length && /^>\s?/.test(lines[index])) {
        quote.push(lines[index].replace(/^>\s?/, ""));
        index += 1;
      }
      html.push(`<blockquote>${inlineMarkdown(quote.join(" "), sourceFile)}</blockquote>`);
      continue;
    }

    if (/^(-{3,}|_{3,}|\*{3,})\s*$/.test(line)) {
      html.push("<hr>");
      index += 1;
      continue;
    }

    const paragraph = [];
    while (index < lines.length) {
      const candidate = lines[index];
      const next = lines[index + 1];
      if (
        !candidate.trim() ||
        /^#{1,4}\s+/.test(candidate) ||
        /^```/.test(candidate) ||
        listItem(candidate) ||
        /^>\s?/.test(candidate) ||
        /^(-{3,}|_{3,}|\*{3,})\s*$/.test(candidate) ||
        (candidate.trim().startsWith("|") && next && isTableDivider(next))
      ) {
        break;
      }
      paragraph.push(candidate.trim());
      index += 1;
    }
    html.push(`<p>${inlineMarkdown(paragraph.join(" "), sourceFile)}</p>`);
  }

  return { html: html.join("\n"), headings };
}

function plainText(markdown) {
  return stripMarkdown(
    markdown
      .replace(/```[\s\S]*?```/g, " ")
      .replace(/^#{1,6}\s+/gm, "")
      .replace(/^\s*[-+*]\s+/gm, "")
      .replace(/^\s*\d+\.\s+/gm, "")
      .replace(/\|/g, " ")
      .replace(/\s+/g, " "),
  );
}

const chapters = chapterFiles.map((file, order) => {
  const markdown = sourceByFile.get(file);
  const firstHeading = markdown.match(/^#\s+(.+)$/m);
  if (!firstHeading) {
    throw new Error(`${file}: missing H1 title`);
  }

  const rendered = renderMarkdown(markdown, file);
  return {
    order,
    file,
    slug: file.replace(/\.md$/, ""),
    title: stripMarkdown(firstHeading[1]),
    html: rendered.html,
    headings: rendered.headings.filter((heading) => heading.level >= 2),
    plainText: plainText(markdown),
    lineCount: markdown.split(/\r?\n/).length,
  };
});

const featureChapter = chapters.find((chapter) => chapter.slug === "03-feature-catalog");
const apiChapter = chapters.find((chapter) => chapter.slug === "04-api-catalog");
const stats = {
  chapters: chapters.length,
  lines: chapters.reduce((sum, chapter) => sum + chapter.lineCount, 0),
  features: featureChapter?.headings.filter((heading) => heading.level === 2).length || 0,
  apiSections: apiChapter?.headings.filter((heading) => heading.level === 2).length || 0,
};

const workflows = [
  {
    slug: "normal-data-transfer",
    title: "Normal Data Transfer",
    shortTitle: "Data Transfer",
    category: "End-to-end data plane",
    summary:
      "A producer record is validated, appended to local storage, delivered to a ready POS consumer, and committed only after the consumer ACKs the batch.",
    scenario:
      "A producer sends a new price for key product-42 on topic prices-v1, and consumer group till-price-service is connected and ready.",
    steps: [
      {
        title: "Producer sends DATA",
        detail:
          "The modern TCP envelope carries JSON with topic, msg_key, event_type, and data. BinaryMessageDecoder creates the broker message.",
        signal: "DATA",
      },
      {
        title: "Broker validates and appends",
        detail:
          "DataHandler requires the topic, creates a MessageRecord, and appends it to partition 0 through StorageEngine.",
        signal: "append(topic, 0, record)",
      },
      {
        title: "Latest-key index advances",
        detail:
          "CompactionIndex.updateKey records the latest offset for product-42 so older versions can be filtered immediately.",
        signal: "key → latestOffset",
      },
      {
        title: "Ready consumer is scheduled",
        detail:
          "Delivery gates check startup readiness, refresh state, storage watermark, in-flight state, and pending ACK state.",
        signal: "DeliveryScheduler",
      },
      {
        title: "Batch is sent",
        detail:
          "BatchDeliveryService sends BATCH_HEADER followed by raw segment bytes, normally using zero-copy FileRegion transfer.",
        signal: "BATCH_HEADER + bytes",
      },
      {
        title: "Consumer processes and ACKs",
        detail:
          "ZeroCopyBatchDecoder emits records to the client handlers. A complete decode produces BATCH_ACK(topic, group).",
        signal: "BATCH_ACK",
      },
      {
        title: "Progress becomes durable",
        detail:
          "BatchAckService commits the next delivery offset and asynchronously writes per-record ACK audit entries to RocksDB.",
        signal: "nextOffset persisted",
      },
    ],
    outcome:
      "The producer receives its append ACK independently of consumer processing. The consumer group advances only after its batch ACK, so disconnect before ACK causes replay.",
    safeguards: [
      "No delivery before READY_ACK.",
      "One pending delivery generation per topic/group.",
      "Partial client batch decode closes the channel without ACK.",
      "Compaction filtering removes known stale versions before send.",
    ],
    sources: [
      "broker/src/main/java/com/messaging/broker/handler/DataHandler.java",
      "broker/src/main/java/com/messaging/broker/consumer/DeliveryScheduler.java",
      "broker/src/main/java/com/messaging/broker/consumer/BatchDeliveryService.java",
      "network/src/main/java/com/messaging/network/codec/ZeroCopyBatchDecoder.java",
      "broker/src/main/java/com/messaging/broker/consumer/BatchAckService.java",
    ],
    related: [
      { label: "Producer flow", route: "#/chapter/06-event-kafka-flow/producer-flow" },
      { label: "Modern delivery flow", route: "#/chapter/06-event-kafka-flow/modern-delivery-flow" },
      { label: "Data model", route: "#/chapter/05-data-model" },
    ],
  },
  {
    slug: "modern-consumer",
    title: "Modern Consumer Flow",
    shortTitle: "Modern Consumer",
    category: "TCP consumer lifecycle",
    summary:
      "A modern consumer registers one topic/group connection, passes the READY handshake, receives zero-copy batches, and reconnects from its durable offset.",
    scenario:
      "The till-price-service group starts for prices-v1 after the broker already contains records.",
    steps: [
      {
        title: "Client discovers handlers",
        detail:
          "ClientConsumerManager finds MessageHandler beans and creates one connection for each configured topic/group.",
        signal: "topic:group",
      },
      {
        title: "Client subscribes",
        detail:
          "SUBSCRIBE carries topic and group. SubscribeHandler restores the persisted group:topic offset and registers the socket.",
        signal: "SUBSCRIBE",
      },
      {
        title: "Startup readiness handshake",
        detail:
          "The broker ACKs subscription, sends READY(topic), and waits until the client calls onReady and returns structured READY_ACK.",
        signal: "READY ↔ READY_ACK",
      },
      {
        title: "Delivery gates open",
        detail:
          "The scheduler reads from the restored offset only when readiness, refresh, watermark, fairness, and pending-delivery gates allow it.",
        signal: "ready = true",
      },
      {
        title: "Client receives a batch",
        detail:
          "The broker sends metadata plus raw record bytes. The decoder invokes every matching topic handler with ConsumerRecord values.",
        signal: "BATCH_HEADER + bytes",
      },
      {
        title: "ACK commits the next offset",
        detail:
          "After full processing, BATCH_ACK lets the broker persist the next-to-deliver offset for that group and topic.",
        signal: "offset = last + 1",
      },
      {
        title: "Reconnect resumes",
        detail:
          "A dead channel triggers reconnect backoff from 5 to 60 seconds. Stale socket state is cleared while the durable offset remains.",
        signal: "5s → 60s backoff",
      },
    ],
    outcome:
      "Each modern group progresses independently. Reconnect replays only work after the last persisted next-to-deliver offset.",
    safeguards: [
      "READY is retried up to three times.",
      "Client IDs may change on reconnect; durable state is keyed by group/topic.",
      "ACK timeout rolls back pending delivery state for replay.",
      "Refresh late joiners follow refresh-specific RESET/READY handling.",
    ],
    sources: [
      "client/src/main/java/com/messaging/client/ClientConsumerManager.java",
      "broker/src/main/java/com/messaging/broker/handler/SubscribeHandler.java",
      "broker/src/main/java/com/messaging/broker/consumer/ConsumerReadinessManager.java",
      "broker/src/main/java/com/messaging/broker/consumer/BatchDeliveryService.java",
    ],
    related: [
      { label: "Modern subscription", route: "#/chapter/06-event-kafka-flow/modern-subscription-flow" },
      { label: "Modern delivery", route: "#/chapter/06-event-kafka-flow/modern-delivery-flow" },
      { label: "POS startup state", route: "#/chapter/07-pos-machine-state/startup-readiness" },
    ],
  },
  {
    slug: "legacy-consumer",
    title: "Legacy Consumer Flow",
    shortTitle: "Legacy Consumer",
    category: "Compatibility data plane",
    summary:
      "A legacy service registers by service name, receives a merged batch across configured topics, and uses generic ACK messages that the broker resolves by FIFO expectation.",
    scenario:
      "A deployed price service registers once and is mapped to prices-v1 plus reference-data-v5.",
    steps: [
      {
        title: "Protocol is detected",
        detail:
          "The first byte must be legacy REGISTER ordinal 0. ProtocolDetectionDecoder switches the channel to legacy codecs.",
        signal: "REGISTER = 0",
      },
      {
        title: "Service name maps to topics",
        detail:
          "The legacy adapter converts registration into internal subscription and LegacyClientConfig resolves the configured service-to-topics map.",
        signal: "service → topics[]",
      },
      {
        title: "Consumers are registered",
        detail:
          "The broker creates one RemoteConsumer per mapped topic while the legacy client keeps one service-level connection.",
        signal: "one socket, many topics",
      },
      {
        title: "READY uses generic ACK",
        detail:
          "The broker sends legacy READY. LegacyConnectionState records the expected response and converts the next generic ACK to READY_ACK.",
        signal: "READY → ACK",
      },
      {
        title: "Topics are merged",
        detail:
          "LegacyConsumerDeliveryManager creates a cursor per topic and performs a priority-queue k-way merge by offset.",
        signal: "k-way offset merge",
      },
      {
        title: "JSON batch is delivered",
        detail:
          "The merged records are encoded as one legacy BatchEvent rather than the modern zero-copy batch format.",
        signal: "BATCH JSON",
      },
      {
        title: "Generic ACK commits all topics",
        detail:
          "FIFO expectation resolves ACK to BATCH_ACK, then the broker commits the maximum included offset for every topic in the batch.",
        signal: "ACK → per-topic progress",
      },
    ],
    outcome:
      "The old client sees one ordered merged stream while the broker preserves separate topic offsets and ACK audit records.",
    safeguards: [
      "Unknown service names do not register.",
      "Pending batch timeout frees the legacy delivery slot.",
      "Disconnect clears readiness and pending expectations.",
      "Exact service aliases are configuration-sensitive.",
    ],
    sources: [
      "network/src/main/java/com/messaging/network/legacy/ProtocolDetectionDecoder.java",
      "network/src/main/java/com/messaging/network/legacy/LegacyConnectionState.java",
      "broker/src/main/java/com/messaging/broker/legacy/LegacyClientConfig.java",
      "broker/src/main/java/com/messaging/broker/legacy/LegacyConsumerDeliveryManager.java",
    ],
    related: [
      { label: "Legacy protocol", route: "#/chapter/06-event-kafka-flow/legacy-wire-protocol" },
      { label: "Legacy delivery", route: "#/chapter/06-event-kafka-flow/legacy-delivery-flow" },
      { label: "Known mapping risk", route: "#/chapter/10-test-map/observed-failures" },
    ],
  },
  {
    slug: "refresh",
    title: "POS Refresh Flow",
    shortTitle: "Refresh",
    category: "Reset, replay, ready",
    summary:
      "Refresh resets selected POS groups, replays the configured local window, and reopens normal operation only after every expected consumer is ready.",
    scenario:
      "An operator refreshes prices-v1 while two POS consumer groups are connected.",
    steps: [
      {
        title: "Admin starts refresh",
        detail:
          "POST /admin/refresh-topic snapshots registered topic/group consumers, captures the replay window target, creates a refresh ID, and persists RESET_SENT.",
        signal: "RESET_SENT",
      },
      {
        title: "Broker sends RESET",
        detail:
          "ACK reconciliation pauses, existing ACK audit entries are cleared for expected groups, and RESET is broadcast with retries every 5 seconds.",
        signal: "RESET → RESET_ACK",
      },
      {
        title: "Offsets reset",
        detail:
          "Each accepted RESET_ACK resets that consumer group's topic offset to the replay start. Legacy consumers use start minus one because their offset means last-delivered.",
        signal: "offset = replay start",
      },
      {
        title: "Snapshot is replayed",
        detail:
          "Normal adaptive batch delivery runs against stable local storage. Local refresh does not pause upstream pipe polling.",
        signal: "REPLAYING",
      },
      {
        title: "Catch-up is confirmed",
        detail:
          "RefreshReplayService requires every expected RESET ACK and every acknowledged consumer offset to reach the captured target.",
        signal: "consumer offset ≥ target",
      },
      {
        title: "Broker sends READY",
        detail:
          "The state becomes READY_SENT. READY is sent to the live reset-ACK audience and retried every 10 seconds when needed.",
        signal: "READY → READY_ACK",
      },
      {
        title: "Normal operation resumes",
        detail:
          "After all READY ACKs, the state becomes COMPLETED and reconciliation resumes. Only destructive download-refresh file operations own pipe pause/resume.",
        signal: "COMPLETED",
      },
    ],
    outcome:
      "Both groups rebuild from the same stable local snapshot before new parent updates are allowed to enter.",
    safeguards: [
      "State is persisted for broker restart recovery.",
      "Late joiners are added to RESET or READY handling based on active state.",
      "A 10-minute watchdog can abort stalled refreshes.",
      "Destructive download refresh pauses pipe polling only while local storage or pipe-offset state is mutated.",
    ],
    sources: [
      "broker/src/main/java/com/messaging/broker/http/RefreshController.java",
      "broker/src/main/java/com/messaging/broker/consumer/RefreshCoordinator.java",
      "broker/src/main/java/com/messaging/broker/consumer/RefreshResetService.java",
      "broker/src/main/java/com/messaging/broker/consumer/RefreshReplayService.java",
      "broker/src/main/java/com/messaging/broker/consumer/RefreshReadyService.java",
    ],
    related: [
      { label: "Refresh state machine", route: "#/chapter/07-pos-machine-state/refresh-state-machine" },
      { label: "Refresh event flow", route: "#/chapter/06-event-kafka-flow/refresh-event-flow" },
      { label: "Refresh API", route: "#/chapter/04-api-catalog/refresh-api" },
      { label: "Planned redesign (P1–P6)", route: "#/chapter/20-download-refresh/planned-redesign-p1p6" },
    ],
  },
  {
    slug: "compaction",
    title: "Compaction Flow",
    shortTitle: "Compaction",
    category: "Latest-key retention",
    summary:
      "The broker tracks the latest offset for each key, filters stale values during delivery, and later rewrites sealed segments while preserving original offsets.",
    scenario:
      "product-42 appears at offsets 100 and 140, then receives a DELETE at 190.",
    steps: [
      {
        title: "Ingress updates latest-key state",
        detail:
          "At offset 140, CompactionIndex marks offset 100 superseded. At 190, the DELETE tombstone becomes the latest state.",
        signal: "product-42 → 190",
      },
      {
        title: "Delivery filtering is immediate",
        detail:
          "Before physical rewrite, BatchDeliveryService drops superseded offsets. Consumers see the latest surviving state rather than old versions.",
        signal: "filter 100, 140",
      },
      {
        title: "Scheduler selects work",
        detail:
          "A single-flight scheduled or manual run checks CPU/heap guards and selects topics with enough sealed segments.",
        signal: "sealed segments only",
      },
      {
        title: "Planner chooses a window",
        detail:
          "CompactionPlanner starts above the stored checkpoint, limits the window size, and always excludes the active segment.",
        signal: "checkpoint → window",
      },
      {
        title: "Segments are rewritten",
        detail:
          "The streaming rewriter copies surviving records, preserves their original offsets, and removes superseded entries.",
        signal: "offset gaps preserved",
      },
      {
        title: "Tombstone retention is applied",
        detail:
          "The latest DELETE remains until its retention period expires. Only then may a later compaction remove that tombstone.",
        signal: "DELETE retained",
      },
      {
        title: "Replacement is published",
        detail:
          "Staging files are forced and atomically renamed, SegmentManager swaps the segment map under a write lock, and checkpoints advance when safe.",
        signal: "atomic replace",
      },
    ],
    outcome:
      "Disk eventually contains only the latest retained state, while consumers continue using the original sparse offsets and remain protected before rewrite.",
    safeguards: [
      "Active segments are excluded unless a manual trigger force-rolls them first.",
      "Streaming rewrite uses a bounded 256 KB buffer.",
      "Atomic staging publication supports recovery.",
      "Compaction is not automatically paused during refresh.",
    ],
    sources: [
      "broker/src/main/java/com/messaging/broker/compaction/CompactionIndex.java",
      "broker/src/main/java/com/messaging/broker/compaction/CompactionPlanner.java",
      "broker/src/main/java/com/messaging/broker/compaction/CompactionRewriter.java",
      "broker/src/main/java/com/messaging/broker/compaction/CompactionScheduler.java",
      "broker/src/main/java/com/messaging/broker/consumer/BatchDeliveryService.java",
    ],
    related: [
      { label: "Compaction handling", route: "#/chapter/08-compaction-handling" },
      { label: "Event semantics", route: "#/chapter/06-event-kafka-flow/compaction-event-semantics" },
      { label: "Compaction API", route: "#/chapter/04-api-catalog/compaction-api" },
    ],
  },
  {
    slug: "pipe-consistency",
    title: "Pipe Consistency Flow",
    shortTitle: "Pipe Consistency",
    category: "Detect-only integrity audit",
    summary:
      "The child compares compaction-invariant keyspace digests with a parent, drills into mismatched buckets, and classifies missing, stale, or zombie keys without repairing data.",
    scenario:
      "A POS was offline long enough to possibly miss both records and expired DELETE tombstones, then an operator checks prices-v1.",
    steps: [
      {
        title: "Check is started",
        detail:
          "An admin POST or enabled scheduler starts one single-flight check. The target is resolved from the current parent or cloud topology.",
        signal: "202 async check",
      },
      {
        title: "Child fixes a watermark",
        detail:
          "The service scans its compaction index at the child head. A lagging parent can clamp comparison to an effective watermark.",
        signal: "child watermark",
      },
      {
        title: "Both sides build bucket digests",
        detail:
          "KeyspaceDigest folds latest key/offset state into 64 XOR buckets. This remains stable even when physical segment compaction differs.",
        signal: "64 digest buckets",
      },
      {
        title: "Equal digest ends quickly",
        detail:
          "If every digest and count matches, the topic is CONSISTENT or CONSISTENT_UP_TO the clamped watermark.",
        signal: "fast path",
      },
      {
        title: "Mismatched buckets are fetched",
        detail:
          "The child requests only mismatched bucket entries, bounded by drill-down and entry limits.",
        signal: "bucket drill-down",
      },
      {
        title: "Differences are classified",
        detail:
          "Parent physical-offset checks and latest-key index state distinguish missed data, stale keys, zombie keys, lagging keys, and child-newer keys.",
        signal: "classify",
      },
      {
        title: "Report is published",
        detail:
          "The bounded report history and Micrometer gauges expose the verdict and whether a refresh is recommended. No repair is performed.",
        signal: "detect, never repair",
      },
    ],
    outcome:
      "Operators receive a per-topic verdict such as CONSISTENT, INCONSISTENT, UNREACHABLE, or INCONCLUSIVE and can decide whether to initiate refresh.",
    safeguards: [
      "Feature scheduler is fail-closed and disabled by default.",
      "Offline parent returns UNREACHABLE without retry loops.",
      "Heap pressure and concurrent-scan guards skip or reject expensive work.",
      "Drill-down and classification sizes are capped.",
    ],
    sources: [
      "broker/src/main/java/com/messaging/broker/consistency/PipeConsistencyService.java",
      "broker/src/main/java/com/messaging/broker/consistency/KeyspaceDigest.java",
      "broker/src/main/java/com/messaging/broker/consistency/ParentConsistencyClient.java",
      "broker/src/main/java/com/messaging/broker/http/PipeConsistencyController.java",
      "broker/src/main/java/com/messaging/broker/http/PipeConsistencyAdminController.java",
    ],
    related: [
      { label: "Feature catalog", route: "#/chapter/03-feature-catalog/pipe-consistency-detection" },
      { label: "Consistency API", route: "#/chapter/04-api-catalog/pipe-consistency-api" },
      { label: "Runtime configuration", route: "#/chapter/11-runtime-config" },
    ],
  },
].map((workflow) => ({
  ...workflow,
  searchText: [
    workflow.title,
    workflow.category,
    workflow.summary,
    workflow.scenario,
    workflow.outcome,
    ...workflow.steps.flatMap((step) => [step.title, step.detail, step.signal]),
    ...workflow.safeguards,
    ...workflow.sources,
  ].join(" "),
}));

const payload = {
  generatedAt: new Date().toISOString(),
  stats,
  chapters,
  workflows,
};

fs.writeFileSync(
  path.join(assetsDir, "content.js"),
  `window.CODEBASE_BOOK = ${JSON.stringify(payload)};\n`,
);

fs.writeFileSync(
  path.join(bookDir, "index.html"),
  `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="0; url=site/index.html">
  <title>Opening Codebase Book…</title>
</head>
<body>
  <p>Opening <a href="site/index.html">Messaging Provider Codebase Book</a>…</p>
  <script>location.replace("site/index.html" + location.hash);</script>
</body>
</html>
`,
);

console.log(
  `Generated ${chapters.length} chapters (${stats.lines} lines, ${stats.features} feature sections) in ${path.relative(process.cwd(), siteDir)}`,
);
