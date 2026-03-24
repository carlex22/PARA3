#!/usr/bin/env python3
"""
ParaAI - Queue Director API (v4.0)
====================================

Responsabilidade: Gerenciar a fila de TAREFAS INDIVIDUAIS via HTTP.

MUDANÇAS v4.0 — Eliminação do clone Git (buraco de minhoca → hyperlink):
  - [BREAKING] GitManager substituído por HttpDirectorManager.
    A NaveMae NÃO clona mais o repositório.
    Toda comunicação com o GitHub é feita via HTTP puro:
      • Startup  → GET CSV de bordo (1 arquivo, ~50 KB)
      • Topup    → GET chunk_dados_{id}.tar.gz sob demanda
      • Reaprov. → GET chunk_llm_{id}.tar.gz (404 = nova chunk)
      • Finaliz. → PUT chunk_llm_{id}.tar.gz + PUT CSV atualizado
  - [BREAKING] Removida dependência de config.py — toda configuração
    é lida diretamente via os.getenv() neste arquivo.
  - [REMOVED] REPO_DIR, QueueManager, GitManager, shutil.
  - [NEW] _bg_push faz upload HTTP direto (sem git push).
  - [NEW] _sync baixa CSV via HTTP e detecta novos chunks pendentes.

Fluxo:
1. Startup → GET CSV → StateManager em WORK_DIR → carrega MIN_CHUNKS_ATIVOS
2. Worker GET /queue/work → recebe 1 task
3. Worker POST /queue/keepalive/{task_id} a cada 30s
4. Worker POST /queue/result/{task_id} → director acumula resultado
5. Chunk completo → merge em memória → PUT tar.gz + PUT CSV no GitHub

Endpoints (todos /queue/* requerem Authorization: Bearer {QUEUE_API_TOKEN}):
    GET  /                          → Dashboard público
    GET  /health                    → Health check público
    GET  /queue/work                → Worker reivindica 1 tarefa
    POST /queue/result/{task_id}    → Worker entrega resultado (sempre aceito)
    POST /queue/keepalive/{task_id} → Worker renova reserva
    GET  /queue/status              → Estatísticas + pressão 429/498
    POST /queue/admin/reload        → Recarrega chunks via HTTP
    POST /queue/admin/requeue/{id}  → Devolve task à fila
    POST /queue/admin/mark_error/{id} → Marca task como erro
"""

import io as _io
import json
import logging
import os
import tarfile as _tarfile
import threading
import time
from collections import deque as _deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-5s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("queue_director")

from workers_tools import StateManager, ResultCollector
from http_director_manager import HttpDirectorManager


# ══════════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO — lida diretamente de variáveis de ambiente
# ══════════════════════════════════════════════════════════════════════

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default

def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default

# Auth / identidade
QUEUE_API_TOKEN   = os.getenv("QUEUE_API_TOKEN", "paraai-secret-token")
GIT_TOKEN         = os.getenv("GIT_TOKEN", "")
GITHUB_REPO_SLUG  = os.getenv("GITHUB_REPO_SLUG", "")          # ex: carlex22/PARA3
GITHUB_BRANCH     = os.getenv("GITHUB_BRANCH", "main")

# Diretórios
WORK_DIR          = Path(os.getenv("WORK_DIR", "./trabalho"))
PROTOCOL_FILE     = Path(os.getenv("PROTOCOL_FILE", "system_role.md"))
STATE_CSV_NAME    = os.getenv("STATE_CSV_NAME", "new_tracking_processamento.csv")

# Intervalos e limites
SYNC_INTERVAL     = _env_int("SYNC_INTERVAL_SEC",   300)
PUSH_INTERVAL     = _env_int("PUSH_INTERVAL_SEC",    30)
TASK_TIMEOUT_MIN  = _env_int("TASK_TIMEOUT_MIN",    240)
ID_MINIMO_CHUNK   = _env_int("ID_MINIMO_CHUNK",    1)

MIN_CHUNKS_ATIVOS      = _env_int("MIN_CHUNKS_ATIVOS",       20)
MAX_CHUNKS_ATIVOS      = _env_int("MAX_CHUNKS_ATIVOS",       35)
BUFFER_MONITOR_INTERVAL = _env_int("BUFFER_MONITOR_INTERVAL", 20)
MAX_RETRIES_PER_TASK   = _env_int("MAX_RETRIES_PER_TASK",      6)
KEEPALIVE_TIMEOUT_SEC  = _env_int("KEEPALIVE_TIMEOUT_SEC",   520)

WORK_DIR.mkdir(parents=True, exist_ok=True)
(WORK_DIR / "dados_llm").mkdir(exist_ok=True)


# ══════════════════════════════════════════════════════════════════════
# ESTADO GLOBAL (thread-safe)
# ══════════════════════════════════════════════════════════════════════

_lock             = threading.Lock()
_task_queue:      List[dict]         = []
_in_progress:     Dict[str, dict]    = {}
_chunk_collectors: Dict[str, object] = {}
_chunk_counters:  Dict[str, dict]    = {}
_pending_push:    List[dict]         = []

_chunk_payload_logs: Dict[str, List[dict]] = {}
_chunk_error_logs:   Dict[str, List[dict]] = {}

_http_manager:  Optional[HttpDirectorManager] = None
_system_prompt: str  = ""
_initialized:   bool = False
_init_error:    str  = ""
_start_time:    Optional[datetime] = None

_stats = {
    "tasks_entregues":         0,
    "tasks_ok":                0,
    "tasks_erro":              0,
    "tasks_skip":              0,
    "tasks_aceitas_sem_reserva": 0,
    "chunks_concluidos":       0,
    "tokens_total":            0,
}

_pressure_429: int = 0
_pressure_498: int = 0

_rpm_window: "_deque[float]"   = _deque()
_tpm_window: "_deque[tuple]"   = _deque()
_WINDOW_SEC = 60
_topup_trigger = threading.Event()


# ══════════════════════════════════════════════════════════════════════
# MÉTRICAS DESLIZANTES
# ══════════════════════════════════════════════════════════════════════

def _record_task_ok(tokens: int):
    now = datetime.utcnow().timestamp()
    _rpm_window.append(now)
    _tpm_window.append((now, tokens))

def _sliding_metrics() -> tuple[float, float]:
    now    = datetime.utcnow().timestamp()
    cutoff = now - _WINDOW_SEC
    while _rpm_window and _rpm_window[0] < cutoff:
        _rpm_window.popleft()
    while _tpm_window and _tpm_window[0][0] < cutoff:
        _tpm_window.popleft()
    rpm = round(len(_rpm_window) / (_WINDOW_SEC / 60), 1)
    tpm = sum(t for _, t in _tpm_window)
    return rpm, tpm


# ══════════════════════════════════════════════════════════════════════
# HELPERS INTERNOS
# ══════════════════════════════════════════════════════════════════════

def _load_system_prompt() -> str:
    if PROTOCOL_FILE.exists():
        return PROTOCOL_FILE.read_text(encoding="utf-8")
    logger.warning("⚠️  system_role.md não encontrado — usando prompt padrão.")
    return "Você é um assistente jurídico especializado em análise de decisões."


def _publish_chunk_llm(chunk_id: str, gz_path: Path, msg: str) -> bool:
    """Lê tar.gz do WORK_DIR e faz upload via GitHub API."""
    try:
        return _http_manager.upload_chunk_llm(chunk_id, gz_path.read_bytes(), msg)
    except Exception as e:
        logger.error(f"❌ [publish] chunk_llm {chunk_id}: {e}")
        return False


def _publish_diario(msg: str = "update tracking") -> bool:
    """Lê CSV do WORK_DIR e faz upload via GitHub API."""
    if True: #try:
        csv_path = WORK_DIR / STATE_CSV_NAME
        if not csv_path.exists():
            logger.error(f"❌ [publish] CSV nao existe ainda")
            return False
        df = pd.read_csv(csv_path)
        return _http_manager.salvar_diario(df, msg)
    #except Exception as e:
    #    logger.error(f"❌ [publish] CSV: {e}")
    #    return False


def _ler_jsonl_de_tar(tar_path: Path) -> list:
    """Extrai jurisprudencias.jsonl de tar.gz local."""
    registros = []
    if not tar_path.exists():
        return registros
    try:
        with _tarfile.open(tar_path, "r:gz") as tar:
            member = next(
                (m for m in tar.getmembers() if m.name.endswith("jurisprudencias.jsonl")), None
            )
            if not member:
                return registros
            for line in tar.extractfile(member):
                line = line.decode("utf-8").strip()
                if line:
                    try:
                        registros.append(json.loads(line))
                    except Exception:
                        continue
    except Exception as e:
        logger.warning(f"⚠️  [tar] {tar_path.name}: {e}")
    return registros


def _ler_jsonl_de_bytes(gz_bytes: bytes) -> list:
    """Extrai jurisprudencias.jsonl de bytes de tar.gz em memória."""
    registros = []
    try:
        with _tarfile.open(fileobj=_io.BytesIO(gz_bytes), mode="r:gz") as tar:
            member = next(
                (m for m in tar.getmembers() if m.name.endswith("jurisprudencias.jsonl")), None
            )
            if not member:
                return registros
            for line in tar.extractfile(member):
                line = line.decode("utf-8").strip()
                if line:
                    try:
                        registros.append(json.loads(line))
                    except Exception:
                        continue
    except Exception as e:
        logger.warning(f"⚠️  [tar/mem] {e}")
    return registros


def _ler_raw_md_de_tar(tar_path: Path) -> dict:
    resultado = {}
    if not tar_path.exists():
        return resultado
    try:
        with _tarfile.open(tar_path, "r:gz") as tar:
            for member in tar.getmembers():
                if member.name.startswith("raw/") and member.name.endswith(".md"):
                    f = tar.extractfile(member)
                    if f:
                        resultado[member.name[4:-3]] = f.read().decode("utf-8")
    except Exception as e:
        logger.warning(f"⚠️  [raw/md] {tar_path.name}: {e}")
    return resultado


def _ler_raw_md_de_bytes(gz_bytes: bytes) -> dict:
    resultado = {}
    try:
        with _tarfile.open(fileobj=_io.BytesIO(gz_bytes), mode="r:gz") as tar:
            for member in tar.getmembers():
                if member.name.startswith("raw/") and member.name.endswith(".md"):
                    f = tar.extractfile(member)
                    if f:
                        resultado[member.name[4:-3]] = f.read().decode("utf-8")
    except Exception as e:
        logger.warning(f"⚠️  [raw/md/mem] {e}")
    return resultado


def _carregar_registros_existentes_llm(chunk_id: str):
    """
    Busca registros já processados: primeiro WORK_DIR, depois GitHub via HTTP.
    Retorna (registros, ids_existentes, por_id).
    """
    work_tar  = WORK_DIR / f"dados_llm/chunk_llm_{chunk_id}.tar.gz"
    registros = _ler_jsonl_de_tar(work_tar)

    if not registros:
        gz_bytes = _http_manager.baixar_chunk_llm(chunk_id)
        if gz_bytes:
            registros = _ler_jsonl_de_bytes(gz_bytes)
            if registros:
                logger.info(f"📂 [reaprov] Chunk {chunk_id}: {len(registros)} registros do GitHub.")

    ids_existentes: set = set()
    por_id:         dict = {}
    for r in registros:
        mj = r.get("manifestacao_juridica", {})
        if not isinstance(mj, dict):
            continue
        mid = mj.get("id_manifestacao")
        if mid is not None:
            key = str(mid)
            ids_existentes.add(key)
            por_id[key] = r

    return registros, ids_existentes, por_id


# ══════════════════════════════════════════════════════════════════════
# CARREGAMENTO DE CHUNKS
# ══════════════════════════════════════════════════════════════════════

def _carregar_chunk_na_fila(chunk_id: str) -> int:
    chunk_id = str(int(chunk_id)).zfill(6)  # normaliza para 6 dígitos (ex: 3335 → 003335)
    """
    [v4.0] Baixa chunk_dados via HTTP, filtra tarefas pendentes e
    injeta pré-existentes diretamente no ResultCollector.
    """
    dados_bytes = _http_manager.baixar_chunk_dados(chunk_id)
    if not dados_bytes:
        logger.warning(f"⚠️  [carregar] Chunk {chunk_id}: não encontrado no GitHub.")
        return 0

    tarefas = _http_manager.extrair_tarefas_de_chunk_dados(chunk_id, dados_bytes)
    if not tarefas:
        logger.warning(f"⚠️  [carregar] Chunk {chunk_id}: sem tarefas válidas.")
        return 0

    _, ids_existentes, existentes_por_id = _carregar_registros_existentes_llm(chunk_id)

    tarefas_faltantes:     List[dict] = []
    tarefas_pre_existentes: List[dict] = []

    for t in tarefas:
        if str(t["id"]) in ids_existentes:
            tarefas_pre_existentes.append(t)
        else:
            tarefas_faltantes.append(t)

    total_original = len(tarefas)
    n_existentes   = len(tarefas_pre_existentes)
    n_faltantes    = len(tarefas_faltantes)

    if n_existentes:
        logger.info(
            f"♻️  [reaprov] Chunk {chunk_id}: "
            f"{n_existentes}/{total_original} pré-existentes | {n_faltantes} novas."
        )
    else:
        logger.info(f"🆕 [carregar] Chunk {chunk_id}: {total_original} tarefas.")

    # Tudo já processado
    if not tarefas_faltantes:
        logger.info(f"✅ [reaprov] Chunk {chunk_id}: completo. Marcando finalizado.")
        sm = StateManager(WORK_DIR, WORK_DIR)
        sm.marcar_chunk_como_finalizado(chunk_id, {"total_ok": n_existentes, "erros": []})
        _publish_diario(f"✅ Chunk {chunk_id} já completo (reaproveitado)")
        return 0

    collector = ResultCollector(WORK_DIR)

    for t in tarefas_pre_existentes:
        mid_str = str(t["id"])
        record  = existentes_por_id.get(mid_str, {})
        eh_erro = (
            isinstance(record.get("manifestacao_juridica"), dict)
            and record["manifestacao_juridica"].get("error")
        )
        collector.adicionar_resultado(
            t,
            {
                "status":            "falha" if eh_erro else "sucesso",
                "resultado":         record,
                "tokens":            0,
                "motivo":            "preexistente_reaproveitado",
                "_raw_markdown":     _build_raw_markdown(t.get("dados_originais", {}), mid_str),
                "_id_manifestacao":  mid_str,
            },
        )

    with _lock:
        _task_queue.extend(tarefas_faltantes)
        _chunk_collectors[chunk_id] = collector
        _chunk_counters[chunk_id]   = {"total": total_original, "done": n_existentes, "tokens": 0}

    logger.info(
        f"📥 Chunk {chunk_id}: {n_faltantes} novas | "
        f"{n_existentes} reaproveitadas | {total_original} total"
    )
    return n_faltantes


def _topup_chunks_se_necessario():
    with _lock:
        ativos = len(_chunk_counters)
        if ativos >= MAX_CHUNKS_ATIVOS:
            return 0
        faltam = MIN_CHUNKS_ATIVOS - ativos
        if faltam <= 0:
            return 0

    logger.info(f"📊 [buffer] {ativos} ativos — carregando {faltam} chunk(s)...")

    sm = StateManager(WORK_DIR, WORK_DIR)
    sm.id_minimo = ID_MINIMO_CHUNK

    carregados = 0
    for _ in range(faltam):
        chunk_id = sm.selecionar_proximo_chunk()
        if not chunk_id:
            logger.info("⏸️  [buffer] Sem chunks pendentes para topup.")
            break
        sm.marcar_chunk_como_iniciado(chunk_id)
        n = _carregar_chunk_na_fila(chunk_id)
        if n > 0:
            carregados += 1
            _publish_diario(f"🔒 LOCK chunk {chunk_id}")
            logger.info(f"✅ [topup] Chunk {chunk_id}: {n} tasks enfileiradas.")
        else:
            sm.marcar_chunk_como_finalizado(chunk_id, {"total_ok": 0, "erros": ["sem_tarefas"]})
            _publish_diario(f"⚠️ Chunk {chunk_id} sem tarefas")

    return carregados


def _sync_e_carregar_proximos_chunks():
    """Baixa CSV via HTTP, atualiza WORK_DIR e carrega próximos chunks pendentes."""
    with _lock:
        if len(_chunk_counters) >= MAX_CHUNKS_ATIVOS:
            logger.info(f"⏸️  [sync] {len(_chunk_counters)} chunks ativos — máximo atingido.")
            return

    logger.info("📥 [sync] Baixando CSV de bordo do GitHub...")
    df = _http_manager.baixar_diario()
    if df is not None:
        df.to_csv(WORK_DIR / STATE_CSV_NAME, index=False)
        logger.info(f"✅ [sync] CSV atualizado: {len(df)} registros.")
    else:
        logger.error("❌ [sync] Falha ao baixar CSV — usando local se disponível.")

    sm = StateManager(WORK_DIR, WORK_DIR)
    sm.id_minimo = ID_MINIMO_CHUNK

    with _lock:
        faltam = max(MIN_CHUNKS_ATIVOS - len(_chunk_counters), 1)

    carregados = 0
    for _ in range(faltam):
        chunk_id = sm.selecionar_proximo_chunk()
        if not chunk_id:
            break
        sm.marcar_chunk_como_iniciado(chunk_id)
        n = _carregar_chunk_na_fila(chunk_id)
        if n > 0:
            carregados += 1
            _publish_diario(f"🔒 LOCK chunk {chunk_id}")
        else:
            sm.marcar_chunk_como_finalizado(chunk_id, {"total_ok": 0, "erros": ["sem_tarefas"]})

    if carregados:
        logger.info(f"✅ [sync] {carregados} chunk(s) carregados via HTTP.")
    else:
        logger.info("⏸️  [sync] am chunk novo disponível.")


# ══════════════════════════════════════════════════════════════════════
# FINALIZAÇÃO DE CHUNK
# ══════════════════════════════════════════════════════════════════════

def _finalizar_chunk_se_completo(chunk_id: str):
    with _lock:
        counter = _chunk_counters.get(chunk_id)
        if not counter or counter["done"] < counter["total"]:
            return
        collector = _chunk_collectors.pop(chunk_id, None)
        del _chunk_counters[chunk_id]

    if not collector:
        return

    logger.info(f"🎉 Chunk {chunk_id} completo ({counter['done']}/{counter['total']})!")
    collector.finalizar_chunk_parcial(chunk_id)
    _pos_processar_jsonl_chunk(chunk_id)
    stats = collector.get_chunk_stats_and_reset()

    sm = StateManager(WORK_DIR, WORK_DIR)
    sm.marcar_chunk_como_finalizado(chunk_id, stats)

    with _lock:
        _pending_push.append({
            "chunk_id": chunk_id,
            "msg": f"✅ Chunk {chunk_id} | {stats['total_ok']} OK | {len(stats['erros'])} erros",
        })
        _stats["chunks_concluidos"] += 1

    logger.info(f"📦 Chunk {chunk_id} enfileirado para upload HTTP.")
    _topup_trigger.set()


def _pos_processar_jsonl_chunk(chunk_id: str):
    """
    Merge do JSONL novo (WORK_DIR) com o existente (GitHub via HTTP).
    Reempacota tar.gz final com: jurisprudencias.jsonl, payloads.jsonl,
    erros.jsonl, raw/<id>.md.
    """
    work_tar = WORK_DIR / f"dados_llm/chunk_llm_{chunk_id}.tar.gz"
    if not work_tar.exists():
        logger.warning(f"⚠️  [merge] {work_tar} não encontrado.")
        return

    def _sort_key(obj):
        mid = obj.get("manifestacao_juridica", {}).get("id_manifestacao") \
            if isinstance(obj.get("manifestacao_juridica"), dict) else None
        try:
            return int(mid) if mid is not None else 0
        except (TypeError, ValueError):
            return 0

    try:
        # Existentes do GitHub via HTTP
        existentes_bytes   = _http_manager.baixar_chunk_llm(chunk_id)
        existentes         = _ler_jsonl_de_bytes(existentes_bytes) if existentes_bytes else []
        raw_md_existentes  = _ler_raw_md_de_bytes(existentes_bytes) if existentes_bytes else {}

        existentes_por_id = {
            str(r["manifestacao_juridica"]["id_manifestacao"]): r
            for r in existentes
            if isinstance(r.get("manifestacao_juridica"), dict)
            and r["manifestacao_juridica"].get("id_manifestacao")
        }

        # Novos do WORK_DIR
        novos = _ler_jsonl_de_tar(work_tar)
        novos_por_id = {
            str(r["manifestacao_juridica"]["id_manifestacao"]): r
            for r in novos
            if isinstance(r.get("manifestacao_juridica"), dict)
            and r["manifestacao_juridica"].get("id_manifestacao")
        }

        merged = {**existentes_por_id, **novos_por_id}
        linhas = sorted(merged.values(), key=_sort_key)

        raw_md_work   = _ler_raw_md_de_tar(work_tar)
        raw_md_merged = {**raw_md_existentes, **raw_md_work}

        logger.info(
            f"🔀 [merge] Chunk {chunk_id}: "
            f"{len(existentes_por_id)} existentes + {len(novos_por_id)} novos "
            f"→ {len(linhas)} registros | {len(raw_md_merged)} raw/*.md"
        )

        dados_bytes = "\n".join(json.dumps(r, ensure_ascii=False) for r in linhas).encode("utf-8")
        work_tar.unlink()

        with _lock:
            payload_logs = _chunk_payload_logs.pop(chunk_id, [])
            error_logs   = _chunk_error_logs.pop(chunk_id, [])

        payloads_bytes = (
            "\n".join(json.dumps(e, ensure_ascii=False) for e in payload_logs).encode("utf-8")
            if payload_logs else b""
        )
        erros_bytes = (
            "\n".join(json.dumps(e, ensure_ascii=False) for e in error_logs).encode("utf-8")
            if error_logs else b""
        )

        with _tarfile.open(work_tar, "w:gz") as tar:
            for name, data in [
                ("jurisprudencias.jsonl", dados_bytes),
                ("payloads.jsonl",        payloads_bytes),
                ("erros.jsonl",           erros_bytes),
            ]:
                if data:
                    info      = _tarfile.TarInfo(name=name)
                    info.size = len(data)
                    tar.addfile(info, _io.BytesIO(data))

            for id_man, md_text in sorted(raw_md_merged.items()):
                md_bytes  = md_text.encode("utf-8")
                info      = _tarfile.TarInfo(name=f"raw/{id_man}.md")
                info.size = len(md_bytes)
                tar.addfile(info, _io.BytesIO(md_bytes))

        logger.info(
            f"📦 [merge] chunk_llm_{chunk_id}.tar.gz: {len(linhas)} registros "
            f"| {len(payload_logs)} payloads | {len(error_logs)} erros "
            f"| {len(raw_md_merged)} raw/*.md"
        )

    except Exception as e:
        logger.error(f"❌ [merge] Chunk {chunk_id}: {e}", exc_info=True)


# ══════════════════════════════════════════════════════════════════════
# TIMEOUT / REQUEUE
# ══════════════════════════════════════════════════════════════════════

def _requeue_timed_out():
    now      = datetime.utcnow()
    deadline = now - timedelta(seconds=KEEPALIVE_TIMEOUT_SEC)

    timed_out_requeue = []
    to_fail           = []

    with _lock:
        for tid, info in list(_in_progress.items()):
            referencia = info.get("last_keepalive") or info["claimed_at"]
            if referencia >= deadline:
                continue
            retries  = info.get("retries", 0)
            elapsed  = int((now - referencia).total_seconds())
            if retries + 1 >= MAX_RETRIES_PER_TASK:
                logger.error(
                    f"⏱️❌ [timeout] Task {tid} | worker={info['worker_id']} | "
                    f"{elapsed}s sem keepalive | {retries+1}/{MAX_RETRIES_PER_TASK} → FALHA DEFINITIVA"
                )
                to_fail.append((tid, info))
                del _in_progress[tid]
            else:
                new_r = retries + 1
                logger.warning(
                    f"⏱️♻️  [timeout] Task {tid} | worker={info['worker_id']} | "
                    f"{elapsed}s sem keepalive | tentativa {new_r}/{MAX_RETRIES_PER_TASK} → requeue"
                )
                info["retries"] = new_r
                timed_out_requeue.append((tid, info["task"], new_r, info["chunk_id"]))
                del _in_progress[tid]

    for tid, task, retries, chunk_id in timed_out_requeue:
        with _lock:
            task["_retries"] = retries
            _task_queue.insert(0, task)

    for tid, info in to_fail:
        chunk_id = info["chunk_id"]
        task     = info["task"]
        with _lock:
            collector = _chunk_collectors.get(chunk_id)
        if collector:
            collector.adicionar_resultado(
                task,
                {
                    "status":   "falha",
                    "resultado": _normalizar_resultado(task, "falha", {}),
                    "tokens":   0,
                    "motivo":   "timeout_keepalive_max_retries",
                },
            )
        with _lock:
            _stats["tasks_erro"] += 1
            if chunk_id in _chunk_counters:
                _chunk_counters[chunk_id]["done"] += 1
        _finalizar_chunk_se_completo(chunk_id)


# ══════════════════════════════════════════════════════════════════════
# NORMALIZAÇÃO / SANITIZAÇÃO
# ══════════════════════════════════════════════════════════════════════

def _limpar_resultado_llm(resultado: dict) -> dict:
    _SCHEMA_KEYS = {"type", "required", "$schema", "$defs", "definitions", "additionalProperties"}

    def _is_schema_wrapper(obj):
        return isinstance(obj, dict) and "properties" in obj and set(obj.keys()) <= _SCHEMA_KEYS | {"properties"}

    def _strip(obj):
        if isinstance(obj, dict):
            return {k: _strip(v) for k, v in obj.items() if k not in _SCHEMA_KEYS}
        if isinstance(obj, list):
            return [_strip(i) for i in obj]
        return obj

    resultado.pop("$schema", None)
    if _is_schema_wrapper(resultado):
        resultado = resultado.get("properties", resultado)
    if "properties" in resultado and "manifestacao_juridica" not in resultado:
        inner = resultado["properties"]
        if isinstance(inner, dict) and "manifestacao_juridica" in inner:
            resultado = inner
    mj = resultado.get("manifestacao_juridica")
    if isinstance(mj, dict) and list(mj.keys()) == ["properties"] and isinstance(mj.get("properties"), dict):
        resultado["manifestacao_juridica"] = mj["properties"]
    return _strip(resultado)


def _normalizar_resultado(task: dict, status: str, resultado: dict) -> dict:
    id_manifestacao = (
        task.get("dados_originais", {})
            .get("manifestacao_juridica", {})
            .get("id_manifestacao")
    )
    if status in ("falha", "skip"):
        return {"manifestacao_juridica": {"id_manifestacao": id_manifestacao, ("error" if status == "falha" else "skip"): True}}
    if id_manifestacao:
        mj = resultado.get("manifestacao_juridica")
        if isinstance(mj, dict):
            if not mj.get("id_manifestacao"):
                mj["id_manifestacao"] = id_manifestacao
        else:
            resultado.setdefault("manifestacao_juridica", {"id_manifestacao": id_manifestacao})
    return resultado


def _build_raw_markdown(dados_originais: dict, id_manifestacao) -> str:
    d        = dados_originais or {}
    num_proc = d.get("numero_processo") or d.get("Id") or str(id_manifestacao)
    linhas   = [
        f"# Acórdão TJPR — id_manifestacao: {id_manifestacao}",
        "",
        f"**Número do processo:** {num_proc}",
    ]
    for label, keys in [
        ("Data do julgamento", ["data_julgamento", "data"]),
        ("Relator",            ["relator", "relator_ou_juiz"]),
        ("Órgão julgador",     ["orgao_julgador", "tribunal"]),
    ]:
        v = next((d.get(k) for k in keys if d.get(k)), None)
        if v:
            linhas.append(f"**{label}:** {v}")
    linhas.append("")
    for section, keys in [
        ("Ementa",              ["ementa"]),
        ("Decisão",             ["decisao"]),
        ("Íntegra do Acórdão",  ["integra_do_acordao", "integra"]),
    ]:
        v = next((d.get(k) for k in keys if d.get(k)), None)
        if v:
            linhas += [f"## {section}", "", v.strip(), ""]
    return "\n".join(linhas)


def _contar_chunks_pendentes() -> int:
    try:
        sm = StateManager(WORK_DIR, WORK_DIR)
        sm.id_minimo = ID_MINIMO_CHUNK
        return sm.contar_chunks_pendentes() if hasattr(sm, "contar_chunks_pendentes") else 0
    except Exception:
        return 0


# ══════════════════════════════════════════════════════════════════════
# BACKGROUND THREADS
# ══════════════════════════════════════════════════════════════════════

def _bg_sync():
    while True:
        time.sleep(SYNC_INTERVAL)
        logger.info("🔄 [BG-sync] Verificando novos chunks via HTTP...")
        try:
            _sync_e_carregar_proximos_chunks()
        except Exception as e:
            logger.error(f"❌ [BG-sync] {e}", exc_info=True)


def _bg_buffer_monitor():
    while True:
        triggered = _topup_trigger.wait(timeout=BUFFER_MONITOR_INTERVAL)
        _topup_trigger.clear()
        try:
            with _lock:
                ativos = len(_chunk_counters)
            if ativos < MIN_CHUNKS_ATIVOS:
                motivo = "sinalizado" if triggered else "periódico"
                logger.info(f"🔔 [buffer] Topup ({motivo}): {ativos}/{MIN_CHUNKS_ATIVOS} ativos.")
                _topup_chunks_se_necessario()
        except Exception as e:
            logger.error(f"❌ [buffer-monitor] {e}", exc_info=True)


def _bg_requeue():
    while True:
        time.sleep(30)
        try:
            _requeue_timed_out()
        except Exception as e:
            logger.error(f"❌ [BG-requeue] {e}", exc_info=True)


def _bg_push():
    """[v4.0] Upload HTTP direto — sem git push."""
    while True:
        time.sleep(PUSH_INTERVAL)
        with _lock:
            pending = list(_pending_push)
            _pending_push.clear()

        if not pending:
            continue

        logger.info(f"🚀 [BG-push] Publicando {len(pending)} chunk(s) via HTTP...")
        for item in pending:
            chunk_id = item["chunk_id"]
            gz_path  = WORK_DIR / f"dados_llm/chunk_llm_{chunk_id}.tar.gz"
            ok_llm   = _publish_chunk_llm(chunk_id, gz_path, item["msg"])
            ok_csv   = _publish_diario(item["msg"]) if ok_llm else False

            if ok_llm and ok_csv:
                logger.info(f"✅ [BG-push] Chunk {chunk_id} publicado.")
            else:
                logger.error(f"❌ [BG-push] Chunk {chunk_id} falhou — re-enfileirando.")
                with _lock:
                    _pending_push.append(item)


# ══════════════════════════════════════════════════════════════════════
# APP FASTAPI
# ══════════════════════════════════════════════════════════════════════

app = FastAPI(title="ParaAI Queue Director", version="4.0.0")


@app.on_event("startup")
async def startup():
    global _http_manager, _system_prompt, _initialized, _init_error, _start_time
    logger.info("=" * 60)
    logger.info("🚀 ParaAI Queue Director v4.0 iniciando...")
    logger.info("=" * 60)
    try:
        if not GIT_TOKEN or not GITHUB_REPO_SLUG:
            raise ValueError("GIT_TOKEN e GITHUB_REPO_SLUG são obrigatórios.")

        _http_manager  = HttpDirectorManager(GIT_TOKEN, GITHUB_REPO_SLUG, GITHUB_BRANCH)
        _system_prompt = _load_system_prompt()

        _sync_e_carregar_proximos_chunks()

        for target, name in [
            (_bg_sync,           "bg-sync"),
            (_bg_push,           "bg-push"),
            (_bg_requeue,        "bg-requeue"),
            (_bg_buffer_monitor, "bg-buffer"),
        ]:
            threading.Thread(target=target, daemon=True, name=name).start()

        _initialized = True
        _start_time  = datetime.utcnow()
        with _lock:
            fila = len(_task_queue)
        logger.info(f"✅ Queue Director pronto. {fila} tasks na fila inicial.")
    except Exception as e:
        _init_error = str(e)
        logger.critical(f"❌ Falha na inicialização: {e}", exc_info=True)


# ══════════════════════════════════════════════════════════════════════
# AUTH
# ══════════════════════════════════════════════════════════════════════

def _require_auth(authorization: str = Header(None)):
    if not authorization or authorization != f"Bearer {QUEUE_API_TOKEN}":
        raise HTTPException(status_code=401, detail="Não autorizado.")


# ══════════════════════════════════════════════════════════════════════
# MODELS
# ══════════════════════════════════════════════════════════════════════

class TaskResult(BaseModel):
    worker_id: str       = "unknown"
    chunk_id:  Optional[str] = None
    status:    str       = "falha"
    resultado: Optional[Any] = None
    tokens:    Optional[Any] = 0
    motivo:    Optional[str] = ""

    class Config:
        extra = "allow"

    @property
    def resultado_safe(self) -> dict:
        return self.resultado if isinstance(self.resultado, dict) else {}

    @property
    def tokens_safe(self) -> int:
        try:
            return int(self.tokens or 0)
        except (TypeError, ValueError):
            return 0


# ══════════════════════════════════════════════════════════════════════
# ROTAS
# ══════════════════════════════════════════════════════════════════════

@app.get("/health")
def health():
    with _lock:
        fila    = len(_task_queue)
        em_proc = len(_in_progress)
    return {
        "status":      "ok" if _initialized else "initializing",
        "error":       _init_error or None,
        "queue_size":  fila,
        "in_progress": em_proc,
        "timestamp":   datetime.utcnow().isoformat(),
    }


@app.get("/queue/status", dependencies=[Depends(_require_auth)])
def queue_status():
    with _lock:
        p429 = _pressure_429
        p498 = _pressure_498
        return {
            "queue_size":    len(_task_queue),
            "in_progress":   len(_in_progress),
            "chunks_ativos": {
                cid: {"total": c["total"], "done": c["done"], "tokens": c.get("tokens", 0)}
                for cid, c in _chunk_counters.items()
            },
            "stats":         dict(_stats),
            "pending_push":  len(_pending_push),
            "pressure":      {"429": p429, "498": p498, "total": p429 * 2 + p498},
            "timestamp":     datetime.utcnow().isoformat(),
        }


@app.get("/queue/work", dependencies=[Depends(_require_auth)])
def get_work(worker_id: str = "unknown"):
    if not _initialized:
        raise HTTPException(status_code=503, detail="Queue Director inicializando.")

    with _lock:
        tarefas_pendentes = [
            (tid, info)
            for tid, info in list(_in_progress.items())
            if info["worker_id"] == worker_id
        ]

        chunks_afetados  = set()
        requeue_tasks:   List[dict] = []

        for tid, info in tarefas_pendentes:
            retries = info.get("retries", 0) + 1
            chunks_afetados.add(info["chunk_id"])
            if retries >= MAX_RETRIES_PER_TASK:
                logger.error(
                    f"⏱️❌ [get_work] Task {tid} | worker {worker_id} | "
                    f"{retries}/{MAX_RETRIES_PER_TASK} tentativas → FALHA."
                )
                collector = _chunk_collectors.get(info["chunk_id"])
                if collector:
                    collector.adicionar_resultado(
                        info["task"],
                        {
                            "status":    "falha",
                            "resultado": _normalizar_resultado(info["task"], "falha", {}),
                            "tokens":    0,
                            "motivo":    "worker_abandonou_max_retries",
                        },
                    )
                _stats["tasks_erro"] += 1
                if info["chunk_id"] in _chunk_counters:
                    _chunk_counters[info["chunk_id"]]["done"] += 1
            else:
                old_task            = info["task"]
                old_task["_retries"] = retries
                requeue_tasks.append(old_task)
                logger.warning(f"♻️  [get_work] Worker {worker_id} pediu nova task sem devolver {tid}. Requeue.")
            del _in_progress[tid]

        # [FIX v3.2] Pop da fila ORIGINAL antes de reinserir as antigas
        if _task_queue:
            task = _task_queue.pop(0)
            for t in reversed(requeue_tasks):
                _task_queue.insert(0, t)
        elif requeue_tasks:
            task = requeue_tasks.pop(0)
            for t in reversed(requeue_tasks):
                _task_queue.insert(0, t)
        else:
            return JSONResponse({"task_id": None, "message": "Fila vazia."})

        task_id  = task["id"]
        chunk_id = task["chunk_id"]
        now      = datetime.utcnow()

        _in_progress[task_id] = {
            "claimed_at":     now,
            "last_keepalive": now,
            "worker_id":      worker_id,
            "chunk_id":       chunk_id,
            "task":           task,
            "retries":        task.pop("_retries", 0),
        }
        _stats["tasks_entregues"] += 1
        logger.info(f"❇️ [get_work] Task {task_id} (chunk {chunk_id}) → worker {worker_id}")

    for cid in chunks_afetados:
        _finalizar_chunk_se_completo(cid)

    return {
        "task_id":         task_id,
        "chunk_id":        chunk_id,
        "dados_originais": task["dados_originais"],
        "system_prompt":   _system_prompt,
    }


@app.post("/queue/keepalive/{task_id}", dependencies=[Depends(_require_auth)])
def keepalive(task_id: str, worker_id: str = "unknown"):
    with _lock:
        info = _in_progress.get(task_id)

    if not info:
        return JSONResponse(
            {"status": "not_found", "task_id": task_id, "message": "Task não reservada."},
            status_code=404,
        )

    with _lock:
        info["last_keepalive"] = datetime.utcnow()

    logger.debug(f"💓 [keepalive] Task {task_id} | worker={worker_id}")
    return {"status": "ok", "task_id": task_id}


@app.post("/queue/result/{task_id}", dependencies=[Depends(_require_auth)])
def post_result(task_id: str, payload: TaskResult):
    if not _initialized:
        raise HTTPException(status_code=503, detail="Queue Director inicializando.")

    wid                  = payload.worker_id or "unknown"
    resultado_normalizado = payload.resultado_safe
    if isinstance(resultado_normalizado, dict):
        resultado_normalizado["instance_id"] = wid

    with _lock:
        info = _in_progress.pop(task_id, None)

    sem_reserva = (info is None)
    if sem_reserva:
        chunk_id = payload.chunk_id
        # normaliza formato (worker envia 003335, CSV pode ter 3335)
        if chunk_id:
            try: chunk_id = str(int(chunk_id)).zfill(6)
            except (ValueError, TypeError): pass
        if not chunk_id:
            logger.warning(f"⚠️  [result] Task {task_id} sem reserva e sem chunk_id — descartado.")
            with _lock:
                _stats["tasks_aceitas_sem_reserva"] += 1
            return {"status": "aceito_sem_reserva", "task_id": task_id, "aviso": "chunk_id ausente"}
        logger.warning(f"⚠️  [result] Task {task_id} sem reserva (chunk={chunk_id}) — aceitando.")
        task_stub = {"id": task_id, "chunk_id": chunk_id, "dados_originais": {}}
        with _lock:
            _stats["tasks_aceitas_sem_reserva"] += 1
    else:
        chunk_id  = info["chunk_id"]
        task_stub = info["task"]

    if payload.status == "sucesso" and isinstance(resultado_normalizado, dict):
        resultado_normalizado = _limpar_resultado_llm(resultado_normalizado)

    resultado_final = _normalizar_resultado(task_stub, payload.status, resultado_normalizado)
    if isinstance(resultado_final, dict):
        resultado_final["instance_id"] = wid

    motivo_lower = (payload.motivo or "").lower()
    with _lock:
        global _pressure_429, _pressure_498
        if "429" in motivo_lower or "rate" in motivo_lower or "too many" in motivo_lower:
            _pressure_429 += 1
        if "498" in motivo_lower:
            _pressure_498 += 1

    extra = payload.model_extra or {}
    if chunk_id:
        wpl = extra.get("_log_payloads") or []
        wel = extra.get("_log_erros")    or []
        with _lock:
            _chunk_payload_logs.setdefault(chunk_id, []).extend(wpl if isinstance(wpl, list) else [])
            _chunk_error_logs.setdefault(chunk_id, []).extend(wel if isinstance(wel, list) else [])

    with _lock:
        collector = _chunk_collectors.get(chunk_id)

    if collector:
        dados_orig   = task_stub.get("dados_originais") or {}
        id_man_raw   = (
            dados_orig.get("manifestacao_juridica", {}).get("id_manifestacao")
            if isinstance(dados_orig.get("manifestacao_juridica"), dict)
            else dados_orig.get("Id") or dados_orig.get("id_manifestacao")
        ) or task_id
        worker_raw_md = extra.get("_raw_markdown") or ""
        worker_id_man = extra.get("_id_manifestacao")
        if worker_id_man:
            id_man_raw = str(worker_id_man)
        raw_md = worker_raw_md or (_build_raw_markdown(dados_orig, id_man_raw) if dados_orig else None)

        collector.adicionar_resultado(
            task_stub,
            {
                "status":           payload.status,
                "resultado":        resultado_final,
                "tokens":           payload.tokens_safe,
                "motivo":           payload.motivo,
                "_raw_markdown":    raw_md,
                "_id_manifestacao": str(id_man_raw),
            },
        )
    else:
        logger.warning(f"⚠️  [result] Collector não encontrado para chunk {chunk_id}.")

    with _lock:
        if payload.status == "sucesso":
            _stats["tasks_ok"] += 1
            _record_task_ok(payload.tokens_safe)
        elif payload.status == "skip":
            _stats["tasks_skip"] += 1
        else:
            _stats["tasks_erro"] += 1
        _stats["tokens_total"] += payload.tokens_safe

        if chunk_id in _chunk_counters:
            cnt = _chunk_counters[chunk_id]
            if not sem_reserva or cnt["done"] < cnt["total"]:
                cnt["done"]   += 1
                cnt["tokens"] += payload.tokens_safe

        done   = _chunk_counters.get(chunk_id, {}).get("done",   "?")
        total  = _chunk_counters.get(chunk_id, {}).get("total",  "?")
        tokens = _chunk_counters.get(chunk_id, {}).get("tokens", 0)

    tag = " [sem-reserva]" if sem_reserva else ""
    logger.info(
        f"📥 [result{tag}] Task {task_id} | {payload.status} | "
        f"chunk {chunk_id} [{done}/{total}] | tokens={payload.tokens_safe} "
        f"(chunk={tokens:,}) | worker={wid}"
    )

    _finalizar_chunk_se_completo(chunk_id)
    return {"status": "aceito", "task_id": task_id}


@app.post("/queue/admin/reload", dependencies=[Depends(_require_auth)])
def admin_reload():
    threading.Thread(target=_sync_e_carregar_proximos_chunks, daemon=True).start()
    return {"status": "reload iniciado"}


@app.post("/queue/admin/requeue/{task_id}", dependencies=[Depends(_require_auth)])
def admin_requeue_task(task_id: str):
    with _lock:
        info = _in_progress.pop(task_id, None)
        if not info:
            raise HTTPException(status_code=404, detail=f"Task {task_id} não está em processamento.")
        task           = info["task"]
        retries        = info.get("retries", 0) + 1
        task["retries"] = retries
        _task_queue.insert(0, task)
    logger.info(f"♻️  [admin] Task {task_id} devolvida à fila (retries={retries})")
    return {"status": "requeued", "task_id": task_id, "retries": retries}


@app.post("/queue/admin/mark_error/{task_id}", dependencies=[Depends(_require_auth)])
def admin_mark_error(task_id: str):
    with _lock:
        info = _in_progress.pop(task_id, None)
        if not info:
            raise HTTPException(status_code=404, detail=f"Task {task_id} não está em processamento.")
        chunk_id  = info["chunk_id"]
        task_stub = info["task"]
        collector = _chunk_collectors.get(chunk_id)
        _stats["tasks_erro"] += 1
    if collector:
        collector.adicionar_resultado(
            task_stub, {"status": "erro", "resultado": {}, "tokens": 0, "motivo": "marcado_erro_manual"}
        )
    logger.warning(f"🚫 [admin] Task {task_id} marcada como erro (chunk={chunk_id})")
    _finalizar_chunk_se_completo(chunk_id)
    return {"status": "marked_error", "task_id": task_id, "chunk_id": chunk_id}


# ══════════════════════════════════════════════════════════════════════
# DASHBOARD HTML
# ══════════════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
def root():
    now = datetime.utcnow()
    with _lock:
        fila         = len(_task_queue)
        em_proc      = len(_in_progress)
        chunks_a     = len(_chunk_counters)
        cnt_snap     = dict(_chunk_counters)
        s            = dict(_stats)
        in_prog_snap = dict(_in_progress)
        p429         = _pressure_429
        p498         = _pressure_498

    buffer_ok    = chunks_a >= MIN_CHUNKS_ATIVOS
    buffer_cor   = "#22c55e" if buffer_ok else "#f59e0b"
    buffer_label = f"{'✅' if buffer_ok else '⚠️'} {chunks_a}/{MIN_CHUNKS_ATIVOS} mín."
    chunks_pend  = _contar_chunks_pendentes()

    rows = ""
    for cid, cnt in sorted(cnt_snap.items()):
        pct     = int(cnt["done"] / cnt["total"] * 100) if cnt["total"] else 0
        bar_cor = "#f59e0b" if (100 - pct) <= 15 else "#38bdf8"
        alert   = " ⚠️" if (100 - pct) <= 15 else ""
        rows   += (
            f"<tr><td>{cid}{alert}</td>"
            f"<td>{cnt['done']} / {cnt['total']} ({pct}%)</td>"
            f"<td><div class='bar-bg'><div class='bar-fill' style='width:{pct}%;background:{bar_cor};'></div></div></td>"
            f"<td style='text-align:right;color:#a855f7;font-size:.8rem'>{cnt.get('tokens',0):,}</td>"
            f"</tr>"
        )
    if not rows:
        rows = "<tr><td colspan='4' style='text-align:center;color:#475569;padding:1.25rem;'>Nenhum chunk ativo</td></tr>"

    tasks_rows = ""
    for tid, info in in_prog_snap.items():
        claimed_at    = info.get("claimed_at", now)
        last_kp       = info.get("last_keepalive") or claimed_at
        elapsed_total = int((now - claimed_at).total_seconds())
        elapsed_kp    = int((now - last_kp).total_seconds())
        retries       = info.get("retries", 0)
        pct_kp        = max(0, min(100, int(elapsed_kp / KEEPALIVE_TIMEOUT_SEC * 100)))
        color         = "#22c55e" if retries == 0 else "#eab308" if retries == 1 else "#f97316" if retries == 2 else "#ef4444"
        bar_color     = "#22c55e" if pct_kp < 60 else "#eab308" if pct_kp < 90 else "#ef4444"
        tasks_rows   += (
            "<tr>"
            f"<td style='color:{color};font-family:monospace;font-size:.78rem'>{tid} / {info.get('chunk_id','?')}</td>"
            f"<td>{info.get('worker_id','?')}</td>"
            f"<td style='color:{color};text-align:center'>{retries}</td>"
            f"<td style='color:#94a3b8;font-size:.75rem'>{elapsed_total}s total | {elapsed_kp}s sem KA</td>"
            "<td><div class='bar-bg'>"
            f"<div class='bar-fill' style='width:{pct_kp}%;background:{bar_color};'></div>"
            "</div></td>"
            f"<td><button onclick=\"adminTask('requeue','{tid}')\" style='margin-right:4px;padding:2px 8px;border-radius:4px;border:none;background:#3b82f6;color:#fff;cursor:pointer;font-size:.75rem'>♻️</button>"
            f"<button onclick=\"adminTask('mark_error','{tid}')\" style='padding:2px 8px;border-radius:4px;border:none;background:#ef4444;color:#fff;cursor:pointer;font-size:.75rem'>🚫</button></td>"
            "</tr>"
        )
    if not tasks_rows:
        tasks_rows = "<tr><td colspan='6' style='text-align:center;color:#475569;padding:1rem;'>Nenhuma tarefa em processamento</td></tr>"

    uptime_sec    = (now - _start_time).total_seconds() if _start_time else 0
    uptime_str    = f"{int(uptime_sec//3600):02d}h {int((uptime_sec%3600)//60):02d}m {int(uptime_sec%60):02d}s"
    rpm, tpm      = _sliding_metrics()
    pt            = p429 * 2 + p498
    pt_color      = "#22c55e" if pt == 0 else "#eab308" if pt < 20 else "#ef4444"

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8"/>
  <meta http-equiv="refresh" content="10"/>
  <title>ParaAI Queue Director v4</title>
  <style>
    *{{margin:0;padding:0;box-sizing:border-box}}
    body{{font-family:-apple-system,sans-serif;background:#0f172a;color:#e2e8f0;padding:2rem}}
    h1{{font-size:1.5rem;color:#f8fafc;margin-bottom:.2rem}}
    .sub{{color:#64748b;font-size:.82rem;margin-bottom:1.25rem}}
    .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:.75rem;margin-bottom:1.5rem}}
    .card{{background:#1e293b;border-radius:.5rem;padding:.9rem;border:1px solid #334155}}
    .lbl{{font-size:.68rem;color:#64748b;text-transform:uppercase;letter-spacing:.04em}}
    .val{{font-size:1.7rem;font-weight:700;margin-top:.15rem}}
    h2{{color:#64748b;font-size:.85rem;margin:1.25rem 0 .5rem}}
    table{{width:100%;border-collapse:collapse;background:#1e293b;border-radius:.5rem;overflow:hidden;border:1px solid #334155}}
    th{{background:#0f172a;padding:.55rem .9rem;font-size:.7rem;color:#64748b;text-align:left}}
    td{{padding:.5rem .9rem;border-top:1px solid #334155;font-size:.84rem}}
    .bar-bg{{background:#1e293b;border-radius:99px;height:6px;width:100%;border:1px solid #334155}}
    .bar-fill{{height:6px;border-radius:99px;transition:width .3s}}
    .ts{{color:#334155;font-size:.72rem;margin-top:1rem}}
  </style>
</head>
<body>
<h1>📋 ParaAI Queue Director <span style="color:#38bdf8;font-size:1rem">v4.0</span></h1>
<p class="sub">Status: {'✅ Pronto' if _initialized else '⏳ Inicializando'} · HTTP-only · auto-refresh 10s</p>
<div class="grid">
  <div class="card"><div class="lbl">Fila</div><div class="val" style="color:#38bdf8">{fila}</div></div>
  <div class="card"><div class="lbl">Em processamento</div><div class="val" style="color:#a855f7">{em_proc}</div></div>
  <div class="card"><div class="lbl">Chunks ativos</div><div class="val" style="color:{buffer_cor}">{buffer_label}</div></div>
  <div class="card"><div class="lbl">Chunks pendentes</div><div class="val" style="color:#64748b">{chunks_pend}</div></div>
  <div class="card"><div class="lbl">Tasks OK</div><div class="val" style="color:#22c55e">{s['tasks_ok']}</div></div>
  <div class="card"><div class="lbl">Tasks erro</div><div class="val" style="color:#f87171">{s['tasks_erro']}</div></div>
  <div class="card"><div class="lbl">Tokens total</div><div class="val" style="color:#a855f7">{s['tokens_total']:,}</div></div>
  <div class="card"><div class="lbl">Reg/min</div><div class="val" style="color:#38bdf8">{rpm}</div></div>
  <div class="card"><div class="lbl">Tok/min</div><div class="val" style="color:#a855f7">{int(tpm):,}</div></div>
  {'<div class="card"><div class="lbl">Pressão 429</div><div class="val" style="color:'+pt_color+'">'+str(p429)+'</div></div>' if p429 else ''}
  {'<div class="card"><div class="lbl">Pressão 498</div><div class="val" style="color:'+pt_color+'">'+str(p498)+'</div></div>' if p498 else ''}
  <div class="card"><div class="lbl">Uptime</div><div class="val" style="color:#64748b;font-size:1.1rem">{uptime_str}</div></div>
</div>
<h2>📦 Chunks ativos</h2>
<table>
  <thead><tr><th>Chunk ID</th><th>Progresso</th><th>Barra</th><th style="text-align:right">Tokens</th></tr></thead>
  <tbody>{rows}</tbody>
</table>
<h2>⚙️ Tarefas em processamento</h2>
<table>
  <thead><tr><th>Task / Chunk</th><th>Worker</th><th>Retries</th><th>Tempo</th><th>KA timer</th><th>Ação</th></tr></thead>
  <tbody>{tasks_rows}</tbody>
</table>
<p class="ts">🕐 {now.strftime('%Y-%m-%d %H:%M:%S')} UTC</p>
<div id="admin-msg" style="position:fixed;bottom:1rem;right:1rem;padding:.6rem 1rem;border-radius:.4rem;background:#1e293b;border:1px solid #334155;font-size:.8rem;color:#94a3b8;display:none;"></div>
<script>
function adminTask(action,tid){{
  if(!confirm(action+'?\\n'+tid))return;
  fetch('/queue/admin/'+action+'/'+encodeURIComponent(tid),{{method:'POST',headers:{{'Authorization':'Bearer {QUEUE_API_TOKEN}'}}}})
    .then(r=>r.json()).then(d=>showAdminMsg(JSON.stringify(d))).catch(e=>showAdminMsg('❌'+e.message));
}}
function showAdminMsg(msg){{
  var el=document.getElementById('admin-msg');
  el.textContent=msg;el.style.display='block';
  setTimeout(()=>{{el.style.display='none';location.reload();}},2000);
}}
</script>
</body>
</html>"""
    return HTMLResponse(html)
