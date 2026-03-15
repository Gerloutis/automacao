const $  = (s) => document.querySelector(s);
const logEl   = $("#log");
const toasts  = $("#toasts");
const progress  = $("#progress");

function toast(title, msg="", type="ok", timeout=4500){
  const el = document.createElement("div");
  el.className = `toast ${type}`;
  el.innerHTML = `<div class="title">${title}</div><div class="msg">${msg}</div>`;
  toasts.appendChild(el); setTimeout(()=>el.remove(), timeout);
}

function hojeStr(){
  const now = new Date();
  const pad = (n)=> String(n).padStart(2,"0");
  const dd = pad(now.getDate());
  const mm = pad(now.getMonth()+1);
  const yyyy = now.getFullYear();
  return `${dd}/${mm}/${yyyy}`;
}

function setHojeChip(){
  const el = document.getElementById("today");
  if (!el) return;
  el.textContent = "Data de hoje: " + hojeStr();
}

async function runTasks(tasks){
  logEl.textContent = "";
  if (!tasks?.length){
    toast("Atenção", "Nenhuma tarefa informada.", "err"); return;
  }
  progress.hidden = false;

  try{
    const res = await fetch("/run", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({
        datas: [hojeStr()],
        tasks
      })
    });
    const json = await res.json();
    logEl.textContent = json.log || "";
    toast(json.ok ? "Concluído" : "Falha", json.ok ? "Execução finalizada." : "Veja o log.", json.ok ? "ok":"err");
  }catch(e){
    logEl.textContent = "Erro de rede/execução: " + (e?.message || e);
    toast("Erro", "Não foi possível executar.", "err", 6000);
  }finally{
    progress.hidden = true;
    document.querySelectorAll(".pill-btn").forEach(b=>b.classList.remove("loading"));
  }
}

async function verify(){
  logEl.textContent = "";
  progress.hidden = false;
  try{
    const res = await fetch("/verify", { method: "GET" });
    const json = await res.json();
    toast(json.ok ? "OK" : "Falha", json.msg || "", json.ok ? "ok":"err");
    logEl.textContent = (json.msg || "") + (json.detail ? ("\n" + json.detail) : "");
  }catch(e){
    toast("Erro", "Não foi possível verificar.", "err");
    logEl.textContent = "Erro na verificação: " + (e?.message || e);
  }finally{
    progress.hidden = true;
  }
}

function wire(){
  setHojeChip();

  const btns = {
    btnVerificar: {
      el: $("#btnVerificar"),
      fn: verify
    },
    btnDES: {
      el: $("#btnDES"),
      tasks: ["des_para_qhc"]
    },
    btnTO: {
      el: $("#btnTO"),
      tasks: ["to_planejamento","whs_to"]
    },
    btnABS: {
      el: $("#btnABS"),
      tasks: ["presenca_abs"]
    },
    btnResumos: {
      el: $("#btnResumos"),
      tasks: ["resumo_to","resumo_abs"]
    }
  };

  Object.values(btns).forEach(cfg=>{
    if (!cfg.el) return;
    cfg.el.addEventListener("click", async ()=>{
      cfg.el.classList.add("loading");
      if (cfg.fn) { await cfg.fn(); cfg.el.classList.remove("loading"); return; }
      await runTasks(cfg.tasks);
    });
  });

  const btnCopy = $("#btnCopiar");
  if (btnCopy){
    btnCopy.addEventListener("click", async ()=>{
      try{
        await navigator.clipboard.writeText(logEl.textContent || "");
        toast("Log copiado", "Conteúdo na área de transferência.");
      }catch(_){
        toast("Não foi possível copiar", "", "err");
      }
    });
  }
}

document.addEventListener("DOMContentLoaded", wire);
