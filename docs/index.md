---
hide:
  - toc
---

<div class="course-hero">
  <div class="course-hero__content">
    <span class="course-eyebrow">Spark · Kafka · Flink · Lakehouse · OLAP</span>
    <h1>Reason about data systems<br><span>the way production does.</span></h1>
    <p class="course-hero__lead">An academy for experienced engineers. Every lesson starts from a workload, then internals, then failure. You leave able to <strong>derive</strong> an architecture — not recite a stack.</p>
    <div class="course-actions">
      <a class="course-button course-button--primary" href="start-here/">Start here <span aria-hidden="true">→</span></a>
      <a class="course-button course-button--secondary" href="how-to-study/">How to study</a>
      <a class="course-button course-button--coffee" href="https://buymeacoffee.com/sanketn">☕ Support this academy</a>
    </div>
    <p class="course-hero__note">Not beginner-friendly — assumes Python, SQL, Docker, and production scars</p>
  </div>
  <div class="course-terminal" aria-label="Academy roadmap">
    <div class="course-terminal__bar"><i></i><i></i><i></i><span>data-engineering / roadmap</span></div>
    <div class="course-terminal__body">
      <p><span class="terminal-muted">00</span> Scale, partitions, shuffle, time</p>
      <p><span class="terminal-muted">01–02</span> Spark + Kafka as the data plane</p>
      <p><span class="terminal-muted">03–04</span> Event time, state, orchestration</p>
      <p><span class="terminal-muted">05–12</span> Storage, query, specialised stores, platform</p>
      <div class="terminal-status"><span></span> Five production systems throughout</div>
    </div>
  </div>
</div>

<div class="course-proof" aria-label="Academy overview">
  <div><strong>13</strong><span>Curriculum phases</span></div>
  <div><strong>5</strong><span>Running production systems</span></div>
  <div><strong>4</strong><span>Interactive simulations</span></div>
  <div><strong>On-call</strong><span>Incident drills, not demos</span></div>
</div>

## Choose a route

<div class="path-grid">
  <a class="path-card" href="learning-paths/#foundations-first">
    <span class="path-card__icon">01</span>
    <span class="path-card__time">Start here if new to internals</span>
    <h3>Foundations first</h3>
    <p>Scale, partitioning, and distributed execution before you touch a product name. Then Kafka and Spark.</p>
    <span class="path-card__link">Follow the foundations path →</span>
  </a>
  <a class="path-card path-card--featured" href="learning-paths/#staff-data-platform">
    <span class="path-card__tag">Most complete</span>
    <span class="path-card__icon">02</span>
    <span class="path-card__time">Staff data platform</span>
    <h3>Derive the whole stack</h3>
    <p>Ingestion through lakehouse, OLAP, quality, and incidents. Workload chooses the technology.</p>
    <span class="path-card__link">Follow the Staff path →</span>
  </a>
  <a class="path-card" href="learning-paths/#on-call-streaming">
    <span class="path-card__icon">03</span>
    <span class="path-card__time">Streaming / on-call</span>
    <h3>Debug lag, skew, watermarks</h3>
    <p>Kafka hot partitions, Spark stragglers, stalled Flink watermarks, ClickHouse ORDER BY mistakes.</p>
    <span class="path-card__link">Follow the on-call path →</span>
  </a>
</div>

## A curriculum built around engineering problems

The phases are named after systems because that is how teams talk. The lessons are named after the problem that forced the system to exist.

<div class="curriculum-grid">
  <div class="curriculum-stage">
    <div class="curriculum-stage__head"><span>Phases 0–3</span><strong>Data plane</strong></div>
    <p>What breaks as volume, time, and fan-out grow — and why a log plus distributed compute is the usual answer.</p>
    <ul>
      <li><a href="foundations/">Foundations</a></li>
      <li><a href="foundations/data-modelling/">Data modelling</a></li>
      <li><a href="spark/">Spark</a></li>
      <li><a href="kafka/">Kafka</a></li>
      <li><a href="flink/">Stream processing</a></li>
    </ul>
  </div>
  <div class="curriculum-stage">
    <div class="curriculum-stage__head"><span>Phases 4–8</span><strong>Storage and query</strong></div>
    <p>Where the table lives, how analysts query it, and why dashboard latency is a storage-layout problem.</p>
    <ul>
      <li><a href="airflow/">Orchestration</a></li>
      <li><a href="lakehouse/">Lakehouse</a></li>
      <li><a href="query-engines/trino/">Trino</a></li>
      <li><a href="olap/clickhouse/">ClickHouse</a></li>
    </ul>
  </div>
  <div class="curriculum-stage">
    <div class="curriculum-stage__head"><span>Phases 9–12</span><strong>Specialised stores and platform</strong></div>
    <p>Access patterns that tabular engines lose: graphs, wide-column, time series, then catalogues, quality, and security.</p>
    <ul>
      <li><a href="time-series/">Time series</a></li>
      <li><a href="databases/">NoSQL</a></li>
      <li><a href="graph/">Graph</a></li>
      <li><a href="metadata/">Platform</a></li>
    </ul>
  </div>
  <div class="curriculum-stage">
    <div class="curriculum-stage__head"><span>Practice</span><strong>Architectures, labs, incidents</strong></div>
    <p>Put the pieces on one diagram. Then break them. Then explain the telemetry.</p>
    <ul>
      <li><a href="architectures/">Architectures</a></li>
      <li><a href="comparisons/">Comparisons</a></li>
      <li><a href="labs/">Labs</a></li>
      <li><a href="incidents/">Incidents</a></li>
      <li><a href="capstone/">Capstone</a></li>
    </ul>
  </div>
</div>

<div class="method-grid">
  <div>
    <span>01</span>
    <h3>Use case → why</h3>
    <p>No dictionary openings. A concrete workload, then the constraint that made the current tool fail.</p>
  </div>
  <div>
    <span>02</span>
    <h3>Internals, not APIs</h3>
    <p>Shuffle files, ISR, watermarks, snapshots, granules. Enough to predict the next incident.</p>
  </div>
  <div>
    <span>03</span>
    <h3>Failure is the exam</h3>
    <p>Gotchas, scale ladders, and incident drills. You have understood it when you can debug it.</p>
  </div>
</div>

<div class="course-cta">
  <div>
    <span class="course-eyebrow">Five running systems</span>
    <h2>Same events. Different engines.</h2>
    <p>SaaS analytics, observability, e-commerce, IoT, and a fraud graph. The schema does not change; the engine does.</p>
  </div>
  <a class="course-button course-button--primary" href="start-here/">Read the briefing <span aria-hidden="true">→</span></a>
</div>

<p class="course-footnote">Sister academies: <a href="https://sanketn26.github.io/learn-ml/">Learn ML</a> · <a href="https://sanketn26.github.io/AIEngineering/">AI Engineering</a> · <a href="https://sanketn26.github.io/interview-prep/">Senior Engineer Academy</a></p>
