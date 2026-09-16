/* Give long lessons a visible first pass without hiding the production depth. */
document$.subscribe(function () {
  const article = document.querySelector("article.md-content__inner");
  const title = article && article.querySelector(":scope > h1");
  if (!article || !title || article.querySelector(".lesson-guide")) return;

  const situation = article.querySelector("#use-case");
  const intuition = article.querySelector("#intuition");
  const internals = article.querySelector("#internals, h2[id^='internals-']");
  const practice = article.querySelector("#practice-the-idea");
  const exercise = article.querySelector("#exercise");
  const outcome = article.querySelector("#what-happened-next");

  // Orientation, architecture, reference, and lab pages already have their own flow.
  if (!intuition || (!situation && !internals)) return;

  const guide = document.createElement("aside");
  guide.className = "lesson-guide";
  guide.setAttribute("aria-label", "Suggested reading route");

  const eyebrow = document.createElement("span");
  eyebrow.className = "lesson-guide__eyebrow";
  eyebrow.textContent = "A simple way through this lesson";

  const copy = document.createElement("p");
  copy.innerHTML = outcome
    ? "On your <strong>first pass</strong>, read the situation, the mental picture, and how it ended. Return for the mechanics and the production detail when you need them."
    : "On your <strong>first pass</strong>, understand the situation and mental picture. Return for mechanics and production detail when you need them.";

  const links = document.createElement("div");
  links.className = "lesson-guide__links";

  // Labels are numbered at render time: any of these sections may be absent.
  const steps = [
    [situation, "Situation"],
    [intuition, "Mental picture"],
    [practice, "Practise it"],
    [internals, "Deep dive"],
    [outcome, "What happened next"],
    [exercise, "Check the idea"],
  ].filter(function ([heading]) {
    return heading;
  });

  steps.forEach(function ([heading, label], index) {
    label = `${index + 1} · ${label}`;
    const link = document.createElement("a");
    link.href = `#${heading.id}`;
    link.textContent = label;
    if (heading === internals) link.className = "lesson-guide__deep-link";
    if (heading === practice) link.className = "lesson-guide__practice-link";
    links.appendChild(link);
  });

  guide.append(eyebrow, copy, links);
  const effort = article.querySelector(":scope > .course-effort");
  (effort || title).insertAdjacentElement("afterend", guide);

  if (internals) {
    const badge = document.createElement("span");
    badge.className = "lesson-depth-badge";
    badge.textContent = "second pass";
    internals.appendChild(badge);
  }

  if (practice) {
    const badge = document.createElement("span");
    badge.className = "lesson-practice-badge";
    badge.textContent = "simulation / lab";
    practice.appendChild(badge);
  }
});
