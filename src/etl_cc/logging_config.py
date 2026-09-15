"""Unified sanitized logging for ETL CC."""
import json, logging, os, re, traceback
from logging.handlers import WatchedFileHandler
from pathlib import Path
SENSITIVE=re.compile(r"password|passwd|secret|token|authorization|api[_-]?key|credential|encryption[_-]?key",re.I)
LOG_PATH=Path(os.getenv("ETL_CC_LOG_FILE","/home/authuser/dev/chatbots/ETL_CC/logs/etl_cc.log"))
_configured=False

def sanitize(v,depth=0):
    if depth>12:return "[MAX_DEPTH]"
    if isinstance(v,dict):return {str(k):("[REDACTED]" if SENSITIVE.search(str(k)) else sanitize(x,depth+1)) for k,x in v.items()}
    if isinstance(v,(list,tuple,set)):return [sanitize(x,depth+1) for x in v]
    if isinstance(v,bytes):return {"type":"bytes","size":len(v),"content":"[BINARY_NOT_LOGGED]"}
    if hasattr(v,"model_dump"):
        try:return sanitize(v.model_dump(mode="json"),depth+1)
        except Exception:return str(v)
    if isinstance(v,(str,int,float,bool)) or v is None:return v
    return str(v)

def configure_logging(component):
    global _configured
    LOG_PATH.parent.mkdir(parents=True,exist_ok=True)
    root=logging.getLogger()
    if not _configured:
        root.setLevel(logging.DEBUG); fmt=logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s","%Y-%m-%d %H:%M:%S")
        fh=WatchedFileHandler(LOG_PATH,encoding="utf-8");fh.setLevel(logging.DEBUG);fh.setFormatter(fmt)
        sh=logging.StreamHandler();sh.setLevel(logging.INFO);sh.setFormatter(fmt)
        root.handlers.clear();root.addHandler(fh);root.addHandler(sh);_configured=True
    return logging.getLogger(component)

def log_event(logger,event,**fields):logger.info("event=%s payload=%s",event,json.dumps(sanitize(fields),default=str,sort_keys=True,separators=(",",":")))
def log_exception(logger,event,exc,**fields):
    fields.update(error_type=type(exc).__name__,error=str(exc),traceback=traceback.format_exc());logger.error("event=%s payload=%s",event,json.dumps(sanitize(fields),default=str,sort_keys=True))
