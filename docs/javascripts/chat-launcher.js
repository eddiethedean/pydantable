/**
 * Floating chat button + modal iframe for pydantable-rag (/chat-app).
 * URL from <meta name="pydantable-chat-app-url"> (set via mkdocs extra.chat_app_url or RAG_CHAT_APP_URL on RTD).
 */
(function () {
  var meta = document.querySelector('meta[name="pydantable-chat-app-url"]');
  var chatUrl = meta && meta.getAttribute("content");
  if (!chatUrl || !String(chatUrl).trim()) return;

  chatUrl = String(chatUrl).trim();

  if (document.getElementById("pydantable-chat-fab")) return;

  var fab = document.createElement("button");
  fab.type = "button";
  fab.id = "pydantable-chat-fab";
  fab.title = "Open documentation assistant";
  fab.setAttribute("aria-label", "Open documentation assistant");
  fab.innerHTML =
    '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>';

  var backdrop = document.createElement("div");
  backdrop.id = "pydantable-chat-backdrop";
  backdrop.setAttribute("role", "dialog");
  backdrop.setAttribute("aria-modal", "true");
  backdrop.setAttribute("aria-labelledby", "pydantable-chat-title");
  backdrop.setAttribute("aria-hidden", "true");

  var dialog = document.createElement("div");
  dialog.id = "pydantable-chat-dialog";

  var header = document.createElement("header");
  var title = document.createElement("span");
  title.id = "pydantable-chat-title";
  title.textContent = "Documentation assistant";
  var closeBtn = document.createElement("button");
  closeBtn.type = "button";
  closeBtn.id = "pydantable-chat-close";
  closeBtn.setAttribute("aria-label", "Close assistant");
  closeBtn.innerHTML = "&times;";

  var frameWrap = document.createElement("div");
  frameWrap.id = "pydantable-chat-frame-wrap";
  var iframe = document.createElement("iframe");
  iframe.id = "pydantable-chat-frame";
  iframe.title = "pydantable documentation assistant";
  iframe.setAttribute(
    "sandbox",
    "allow-scripts allow-same-origin allow-forms allow-popups"
  );
  iframe.loading = "lazy";

  header.appendChild(title);
  header.appendChild(closeBtn);
  frameWrap.appendChild(iframe);
  dialog.appendChild(header);
  dialog.appendChild(frameWrap);
  backdrop.appendChild(dialog);

  var loaded = false;

  function openModal() {
    backdrop.setAttribute("aria-hidden", "false");
    document.body.style.overflow = "hidden";
    if (!loaded) {
      iframe.src = chatUrl;
      loaded = true;
    }
    closeBtn.focus();
  }

  function closeModal() {
    backdrop.setAttribute("aria-hidden", "true");
    document.body.style.overflow = "";
    fab.focus();
  }

  fab.addEventListener("click", function () {
    openModal();
  });
  closeBtn.addEventListener("click", function () {
    closeModal();
  });
  backdrop.addEventListener("click", function (ev) {
    if (ev.target === backdrop) closeModal();
  });

  document.addEventListener("keydown", function (ev) {
    if (ev.key === "Escape" && backdrop.getAttribute("aria-hidden") === "false") {
      closeModal();
    }
  });

  document.body.appendChild(fab);
  document.body.appendChild(backdrop);
})();
