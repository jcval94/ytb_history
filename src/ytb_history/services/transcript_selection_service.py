"""Deterministic daily selection of transcription candidates."""
from __future__ import annotations
import csv, json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ytb_history.services.transcript_store_service import list_transcribed_video_ids
from ytb_history.storage.jsonl import read_jsonl_gz
from ytb_history.utils.video_ids import is_transcribable_video_id_candidate
DEFAULT_TRANSCRIPTION_CHANNEL_URLS = [
    "https://www.youtube.com/@bilinkis",
    "https://www.youtube.com/veritasium",
]


def load_forced_transcription_channel_urls() -> list[str]:
    cfg = Path("config/transcription_channels.py")
    if not cfg.exists():
        return DEFAULT_TRANSCRIPTION_CHANNEL_URLS
    namespace: dict[str, Any] = {}
    exec(cfg.read_text(encoding="utf-8"), namespace)  # controlled local config file
    urls = namespace.get("TRANSCRIPTION_CHANNEL_URLS", DEFAULT_TRANSCRIPTION_CHANNEL_URLS)
    return list(urls) if isinstance(urls, list) else DEFAULT_TRANSCRIPTION_CHANNEL_URLS

_load_forced_urls = load_forced_transcription_channel_urls
DEFAULT_LIMIT=10
DEFAULT_COOLDOWN_DAYS=7
DEFAULT_FORCED_MAX_PER_RUN=50
DEFAULT_FORCED_WINDOW_DAYS=14

TRANSCRIPT_RETRY_COOLDOWN_STATUSES = {
    "failed",
    "failed_audio_download",
    "failed_audio_download_auth_required",
    "failed_audio_download_video_unavailable",
    "failed_audio_download_network_or_rate_limit",
}

def _is_retry_cooldown_status(status: str) -> bool:
    return status in TRANSCRIPT_RETRY_COOLDOWN_STATUSES or status.startswith("failed_")


def _read_csv(path: Path)->list[dict[str,str]]:
    if not path.exists(): return []
    with path.open("r",encoding="utf-8",newline="") as h: return list(csv.DictReader(h))

def _read_jsonl(path: Path)->list[dict[str,Any]]:
    if not path.exists(): return []
    out=[]
    for line in path.read_text(encoding='utf-8').splitlines():
        if line.strip(): out.append(json.loads(line))
    return out

def _write_jsonl(path:Path,rows:list[dict[str,Any]])->None:
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text("".join(json.dumps(r,ensure_ascii=False)+"\n" for r in rows),encoding='utf-8')

def _source_artifact_summary(root: Path) -> dict[str, dict[str, Any]]:
    paths = {
        "decision": root / "decision" / "latest_action_candidates.csv",
        "model_intelligence": root / "model_intelligence" / "latest_hybrid_recommendations.csv",
        "topic": root / "topic_intelligence" / "latest_topic_opportunities.csv",
        "creative": root / "creative_packages" / "latest_creative_packages.csv",
        "analytics_scores": root / "analytics" / "latest" / "latest_video_scores.csv",
        "analytics_metrics": root / "analytics" / "latest" / "latest_video_metrics.csv",
    }
    summary: dict[str, dict[str, Any]] = {}
    for name, path in paths.items():
        exists = path.exists()
        item: dict[str, Any] = {
            "path": str(path),
            "exists": exists,
            "row_count": len(_read_csv(path)) if exists else 0,
            "modified_at": None,
        }
        if exists:
            item["modified_at"] = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()
        summary[name] = item
    latest_snapshot = _latest_snapshot_path(root)
    summary["latest_snapshots"] = {
        "path": str(latest_snapshot) if latest_snapshot else "",
        "exists": latest_snapshot is not None,
        "row_count": len(_read_latest_snapshot_rows(root)) if latest_snapshot else 0,
        "modified_at": datetime.fromtimestamp(latest_snapshot.stat().st_mtime, tz=timezone.utc).isoformat() if latest_snapshot else None,
    }
    return summary

def _latest_snapshot_path(root: Path) -> Path | None:
    snapshots_root = root / "snapshots"
    if not snapshots_root.exists():
        return None
    candidates = [path for path in snapshots_root.rglob("snapshots.jsonl.gz") if path.is_file()]
    return max(candidates, key=lambda path: (path.parent.parent.name, path.parent.name)) if candidates else None

def _read_latest_snapshot_rows(root: Path)->list[dict[str,Any]]:
    path = _latest_snapshot_path(root)
    return read_jsonl_gz(path) if path else []

def _safe_float(v:Any)->float|None:
    try: return None if v in (None,"") else float(str(v))
    except: return None

def _parse_dt(v:str|None)->datetime|None:
    if not v: return None
    try:
        dt=datetime.fromisoformat(v.replace('Z','+00:00'))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except: return None

def _merge(store:dict[str,dict[str,Any]], row:dict[str,str], mapping:dict[str,str], source:str)->str|None:
    vid=(row.get(mapping.get('video_id','video_id'),"") or "").strip()
    if not vid:return None
    if not is_transcribable_video_id_candidate(vid): return vid
    item=store.setdefault(vid,{"video_id":vid,"sources":set()})
    item['sources'].add(source)
    for tk,sk in mapping.items():
        val=row.get(sk,"")
        if tk.endswith('_score'):
            n=_safe_float(val)
            if n is not None: item[tk]=n
        elif val and not item.get(tk): item[tk]=val
    return None

def _score(c:dict[str,Any])->float:
    w={"decision_score":0.30,"hybrid_decision_score":0.20,"creative_execution_score":0.20,"topic_opportunity_score":0.15,"alpha_score":0.10,"metric_confidence_score":0.05}
    a={k:v for k,v in w.items() if c.get(k) is not None}
    if not a:return 0.0
    s=sum(float(c[k])*(wv/sum(a.values())) for k,wv in a.items())
    return round(max(0.0,min(100.0,s)),4)

def _url_handle(url:str)->str:
    u=url.lower().rstrip('/')
    if '/@' in u: return u.split('/@',1)[1]
    return u.rsplit('/',1)[-1]

def _forced_channel_ids(root:Path, forced_urls:list[str])->dict[str,str]:
    handles={_url_handle(u):u for u in forced_urls}
    out:dict[str,str]={}
    for row in _read_jsonl(root/'state'/'channel_registry.jsonl'):
        if str(row.get('resolver_status','ok'))!='ok': continue
        cid=str(row.get('channel_id','')).strip()
        if not cid: continue
        row_handle=_url_handle(str(row.get('channel_url','')))
        matched=handles.get(row_handle)
        if matched: out[cid]=matched
    return out

def select_transcription_candidates(*,data_dir:str|Path='data',limit:int=DEFAULT_LIMIT,cooldown_days:int=DEFAULT_COOLDOWN_DAYS,forced_channels_enabled:bool=True,forced_channels_max_per_run:int=DEFAULT_FORCED_MAX_PER_RUN,forced_channels_new_video_window_days:int=DEFAULT_FORCED_WINDOW_DAYS)->dict[str,Any]:
    root=Path(data_dir); tdir=root/'transcripts'; now=datetime.now(timezone.utc); now_iso=now.isoformat(); warnings=[]
    cands:dict[str,dict[str,Any]]={}
    skipped_invalid_video_ids: list[dict[str,str]]=[]
    source_artifacts = _source_artifact_summary(root)
    for row in _read_csv(root/'decision'/'latest_action_candidates.csv'):
        invalid_id=_merge(cands,row,{"video_id":"video_id","channel_id":"channel_id","channel_name":"channel_name","title":"title","upload_date":"upload_date","content_format":"content_format","decision_score":"decision_score","alpha_score":"alpha_score","metric_confidence_score":"metric_confidence_score"},"decision")
        if invalid_id: skipped_invalid_video_ids.append({"video_id":invalid_id,"source":"decision"})
    for row in _read_csv(root/'model_intelligence'/'latest_hybrid_recommendations.csv'):
        invalid_id=_merge(cands,row,{"video_id":"video_id","content_format":"content_format","hybrid_decision_score":"hybrid_decision_score"},"model_intelligence")
        if invalid_id: skipped_invalid_video_ids.append({"video_id":invalid_id,"source":"model_intelligence"})
    for row in _read_csv(root/'topic_intelligence'/'latest_topic_opportunities.csv'):
        invalid_id=_merge(cands,row,{"video_id":"video_id","content_format":"content_format","topic_opportunity_score":"topic_opportunity_score"},"topic")
        if invalid_id: skipped_invalid_video_ids.append({"video_id":invalid_id,"source":"topic"})
    for row in _read_csv(root/'creative_packages'/'latest_creative_packages.csv'):
        invalid_id=_merge(cands,row,{"video_id":"source_video_id","content_format":"content_format","creative_execution_score":"creative_execution_score"},"creative")
        if invalid_id: skipped_invalid_video_ids.append({"video_id":invalid_id,"source":"creative"})
    for row in _read_csv(root/'analytics'/'latest'/'latest_video_scores.csv'):
        invalid_id=_merge(cands,row,{"video_id":"video_id","content_format":"content_format","alpha_score":"alpha_score"},"analytics_scores")
        if invalid_id: skipped_invalid_video_ids.append({"video_id":invalid_id,"source":"analytics_scores"})
    metrics=_read_csv(root/'analytics'/'latest'/'latest_video_metrics.csv')
    for row in metrics:
        invalid_id=_merge(cands,row,{"video_id":"video_id","channel_id":"channel_id","channel_name":"channel_name","title":"title","upload_date":"upload_date","content_format":"content_format"},"analytics_metrics")
        if invalid_id: skipped_invalid_video_ids.append({"video_id":invalid_id,"source":"analytics_metrics"})
    for row in _read_latest_snapshot_rows(root):
        invalid_id=_merge(cands,{k:str(v) if v is not None else "" for k,v in row.items()},{"video_id":"video_id","channel_id":"channel_id","channel_name":"channel_name","title":"title","upload_date":"upload_date","content_format":"content_format"},"latest_snapshots")
        if invalid_id: skipped_invalid_video_ids.append({"video_id":invalid_id,"source":"latest_snapshots"})

    reg=_read_jsonl(tdir/'transcript_registry.jsonl'); registry_success=set(); progress=set(); cool=set(); th=now-timedelta(days=cooldown_days)
    for e in reg:
        vid=str(e.get('video_id','')).strip(); st=str(e.get('status','')).strip()
        if not vid: continue
        if st=='success': registry_success.add(vid)
        elif st=='in_progress': progress.add(vid)
        elif _is_retry_cooldown_status(st):
            fa=_parse_dt(str(e.get('failed_at',''))) or _parse_dt(str(e.get('selected_at','')))
            if fa and fa>=th: cool.add(vid)
    success=list_transcribed_video_ids(data_dir=root)

    forced_urls = _load_forced_urls()
    forced_handles={_url_handle(u):u for u in forced_urls}
    forced_ids_by_channel=_forced_channel_ids(root, forced_urls)
    forced_rows=[]
    skipped_forced_success=skipped_forced_progress=skipped_forced_failed=0
    skipped_forced_outside_window=0
    forced_candidate_pool_count=0
    forced_match_counts: dict[str, int] = {url: 0 for url in forced_urls}
    parsed_upload_dates=[_parse_dt(str(item.get('upload_date',''))) for item in cands.values()]
    latest_upload_date=max((d.date() for d in parsed_upload_dates if d), default=None)
    cutoff_date=(now-timedelta(days=forced_channels_new_video_window_days)).date()
    if latest_upload_date:
        anchored_cutoff=latest_upload_date-timedelta(days=forced_channels_new_video_window_days)
        if anchored_cutoff!=cutoff_date:
            warnings.append('forced_channels_window_anchored_to_latest_upload_date')
        cutoff_date=anchored_cutoff
    if forced_channels_enabled and forced_handles:
        for vid,item in cands.items():
            cname=str(item.get('channel_name','')).lower()
            matched=forced_ids_by_channel.get(str(item.get('channel_id','')).strip())
            for h,url in forced_handles.items():
                if matched: break
                if h and h in cname:
                    matched=url; break
            if not matched: continue
            forced_candidate_pool_count+=1
            forced_match_counts[matched]=forced_match_counts.get(matched,0)+1
            if vid in success: skipped_forced_success+=1; continue
            if vid in progress: skipped_forced_progress+=1; continue
            if vid in cool: skipped_forced_failed+=1; continue
            ud=_parse_dt(str(item.get('upload_date','')))
            if ud and ud.date()<cutoff_date:
                skipped_forced_outside_window+=1
                continue
            row={"video_id":vid,"channel_id":item.get("channel_id",""),"channel_name":item.get("channel_name",""),"title":item.get("title",""),"upload_date":item.get("upload_date",""),"content_format":item.get("content_format",""),"decision_score":item.get("decision_score"),"hybrid_decision_score":item.get("hybrid_decision_score"),"creative_execution_score":item.get("creative_execution_score"),"topic_opportunity_score":item.get("topic_opportunity_score"),"alpha_score":item.get("alpha_score"),"metric_confidence_score":item.get("metric_confidence_score"),"source_reason":"forced_channel_new_video","selection_source":"forced_channel_new_video","forced_channel":True,"forced_channel_url":matched,"evidence_json":{"sources":sorted(item.get('sources',set()))}}
            row['transcription_value_score']=_score(row); forced_rows.append(row)
        forced_rows.sort(key=lambda r:(-(_parse_dt(str(r.get('upload_date',''))) or datetime.min.replace(tzinfo=timezone.utc)).timestamp(),-float(r['transcription_value_score']),r['video_id']))
        available_forced_before_truncation=len(forced_rows)
        if len(forced_rows)>forced_channels_max_per_run:
            warnings.append('forced_channels_truncated_max_per_run')
            forced_rows=forced_rows[:forced_channels_max_per_run]
    else:
        available_forced_before_truncation=0
    forced_channels_without_matches=[url for url,count in forced_match_counts.items() if count==0]
    if forced_channels_enabled and forced_channels_without_matches:
        warnings.append('forced_channels_without_matches')

    ranked=[]; skipped_success=skipped_progress=skipped_fail=0
    forced_ids={r['video_id'] for r in forced_rows}
    for vid,item in cands.items():
        if vid in forced_ids: continue
        if vid in success: skipped_success+=1; continue
        if vid in progress: skipped_progress+=1; continue
        if vid in cool: skipped_fail+=1; continue
        row={"video_id":vid,"channel_id":item.get("channel_id",""),"channel_name":item.get("channel_name",""),"title":item.get("title",""),"upload_date":item.get("upload_date",""),"content_format":item.get("content_format",""),"decision_score":item.get("decision_score"),"hybrid_decision_score":item.get("hybrid_decision_score"),"creative_execution_score":item.get("creative_execution_score"),"topic_opportunity_score":item.get("topic_opportunity_score"),"alpha_score":item.get("alpha_score"),"metric_confidence_score":item.get("metric_confidence_score"),"source_reason":"top_daily_value","selection_source":"ranked_daily_top","forced_channel":False,"forced_channel_url":None,"evidence_json":{"sources":sorted(item.get('sources',set()))}}
        row['transcription_value_score']=_score(row); ranked.append(row)
    ranked.sort(key=lambda r:(-float(r['transcription_value_score']),r['video_id']))
    available_ranked_count=len(ranked)
    ranked=ranked[:max(0,limit)]
    ranked_shortfall=max(0, limit-len(ranked))
    if ranked_shortfall:
        warnings.append('ranked_selection_shortfall')

    selected=forced_rows+ranked
    dedup_selected=[]; seen_selected=set(); skipped_duplicate_selected=0
    for row in selected:
        vid=str(row.get("video_id","")).strip()
        if not vid: continue
        if vid in seen_selected:
            skipped_duplicate_selected+=1
            continue
        seen_selected.add(vid); dedup_selected.append(row)
    selected=dedup_selected
    q=[]
    for i,row in enumerate(selected,1):
        q.append({"selected_at":now_iso,"video_id":row['video_id'],"channel_id":row['channel_id'],"channel_name":row['channel_name'],"title":row['title'],"upload_date":row['upload_date'],"content_format":row.get('content_format',''),"selection_rank":i,"selection_source":row['selection_source'],"forced_channel":row['forced_channel'],"forced_channel_url":row['forced_channel_url'],"transcription_value_score":row['transcription_value_score'],"decision_score":row.get('decision_score'),"hybrid_decision_score":row.get('hybrid_decision_score'),"creative_execution_score":row.get('creative_execution_score'),"topic_opportunity_score":row.get('topic_opportunity_score'),"alpha_score":row.get('alpha_score'),"metric_confidence_score":row.get('metric_confidence_score'),"status":"queued","source_reason":row['source_reason'],"evidence_json":row['evidence_json']})
    _write_jsonl(tdir/'transcript_queue.jsonl',q)
    if skipped_invalid_video_ids:
        warnings.append('invalid_video_ids_skipped')
    expected_selected_count=limit+len(forced_rows)
    rep={"generated_at":now_iso,"limit":limit,"expected_ranked_count":limit,"expected_selected_count":expected_selected_count,"ranked_shortfall":ranked_shortfall,"candidates_considered":len(cands),"available_ranked_count":available_ranked_count,"source_artifacts":source_artifacts,"selected_count":len(q),"selected_forced_count":len([r for r in q if r.get('forced_channel')]),"selected_ranked_count":len([r for r in q if not r.get('forced_channel')]),"forced_channels_configured":list(forced_urls),"forced_channels_matched":sorted({r['forced_channel_url'] for r in forced_rows if r.get('forced_channel_url')}),"forced_channels_without_matches":forced_channels_without_matches,"forced_match_counts":forced_match_counts,"forced_candidate_pool_count":forced_candidate_pool_count,"available_forced_before_truncation":available_forced_before_truncation,"forced_window_cutoff_date":cutoff_date.isoformat(),"latest_upload_date":latest_upload_date.isoformat() if latest_upload_date else None,"forced_channels_max_per_run":forced_channels_max_per_run,"registry_existing_success_count":len(registry_success),"persisted_transcript_success_count":len(success),"artifact_only_success_count":len(success-registry_success),"registry_existing_in_progress_count":len(progress),"registry_existing_recent_failed_count":len(cool),"skipped_already_transcribed":skipped_success,"skipped_in_progress":skipped_progress,"skipped_recent_failures":skipped_fail,"skipped_forced_already_transcribed":skipped_forced_success,"skipped_forced_in_progress":skipped_forced_progress,"skipped_forced_recent_failures":skipped_forced_failed,"skipped_forced_outside_window":skipped_forced_outside_window,"skipped_duplicate_selected":skipped_duplicate_selected,"skipped_invalid_video_id_count":len(skipped_invalid_video_ids),"skipped_invalid_video_ids":skipped_invalid_video_ids[:50],"top_selected":[r['video_id'] for r in q[:5]],"warnings":warnings}
    (tdir/'transcript_selection_report.json').write_text(json.dumps(rep,ensure_ascii=False,indent=2),encoding='utf-8')
    return rep
