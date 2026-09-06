/* Add a lightweight effort estimate to substantial lesson pages. */
document$.subscribe(function () {
  const article = document.querySelector("article.md-content__inner");
  const heading = article && article.querySelector(":scope > h1");
  if (!article || !heading || article.querySelector(".course-effort")) return;

  const clone = article.cloneNode(true);
  clone.querySelectorAll("pre, code, nav, script, style").forEach((node) => node.remove());
  const words = (clone.textContent.match(/\b[\w’-]+\b/g) || []).length;
  if (words < 400) return;

  const minutes = Math.max(5, Math.ceil(words / 220 / 5) * 5);
  const effort = document.createElement("p");
  effort.className = "course-effort";
  effort.textContent = `Full-page reading time: about ${minutes} minutes. Use the first-pass route below if you want the idea before the detail.`;
  heading.insertAdjacentElement("afterend", effort);
});
