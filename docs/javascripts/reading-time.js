/* Add a lightweight effort estimate to substantial lesson pages. */
document$.subscribe(function () {
  const article = document.querySelector("article.md-content__inner");
  const heading = article && article.querySelector(":scope > h1");
  if (!article || !heading || article.querySelector(".course-effort")) return;

  const clone = article.cloneNode(true);
  clone.querySelectorAll("pre, code, nav, script, style").forEach((node) => node.remove());

  const countWords = (node) => (node.textContent.match(/\b[\w’-]+\b/g) || []).length;
  const words = countWords(clone);
  if (words < 400) return;

  // Everything from the "Reference" heading onward is third-pass material that
  // sits after the exercise, so it should not inflate the lesson's own estimate.
  let referenceWords = 0;
  const referenceHeading = Array.from(clone.querySelectorAll("h2")).find(
    (h2) => h2.textContent.trim().replace(/¶$/, "").trim() === "Reference"
  );
  if (referenceHeading) {
    let node = referenceHeading;
    while (node) {
      referenceWords += countWords(node);
      node = node.nextElementSibling;
    }
  }

  const toMinutes = (count) => Math.max(5, Math.ceil(count / 220 / 5) * 5);
  const minutes = toMinutes(words - referenceWords);
  const effort = document.createElement("p");
  effort.className = "course-effort";
  effort.textContent = referenceWords
    ? `Lesson reading time: about ${minutes} minutes, plus about ${toMinutes(referenceWords)} minutes of reference after the exercise. Use the first-pass route below if you want the idea before the detail.`
    : `Full-page reading time: about ${minutes} minutes. Use the first-pass route below if you want the idea before the detail.`;
  heading.insertAdjacentElement("afterend", effort);
});
