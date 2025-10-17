# dripper.py  (run as a Python script task)
# Params via job params or env vars
CAT = dbutils.widgets.get("CAT")
SCH = dbutils.widgets.get("SCH")
SRC_VOL = dbutils.widgets.get("SRC_VOL")        # e.g., full_history
DST_VOL = dbutils.widgets.get("DST_VOL")        # e.g., landing
N_PER_RUN = int(dbutils.widgets.get("N_PER_RUN")) if "N_PER_RUN" in [w.name for w in dbutils.widgets.getArgumentInfo()] else 5

import time, uuid, datetime

src_root = f"/Volumes/{CAT}/{SCH}/{SRC_VOL}"
dst_root = f"/Volumes/{CAT}/{SCH}/{DST_VOL}"
staging = f"{dst_root}/_staging"

def now_parts():
    utc = datetime.datetime.utcnow()
    return f"dt={utc.date()}/hr={utc.hour}"

# ensure staging dir exists
dbutils.fs.mkdirs(staging)

# list a batch from a holding/backlog folder
candidates = [f for f in dbutils.fs.ls(src_root) if f.path.endswith(".csv")]
candidates = sorted(candidates, key=lambda x: x.name)[:N_PER_RUN]

for f in candidates:
    # copy to staging so readers never see partials; cp on cloud storage creates a full object
    tmp_name = f"{uuid.uuid4().hex}.csv"
    dbutils.fs.cp(f.path, f"{staging}/{tmp_name}")  # copy
    # move into a time-partitioned folder in landing
    rel = now_parts()
    dest_dir = f"{dst_root}/{rel}"
    dbutils.fs.mkdirs(dest_dir)
    dbutils.fs.mv(f"{staging}/{tmp_name}", f"{dest_dir}/{f.name}")  # move within landing
    # optionally delete source (or keep as archive)
    dbutils.fs.rm(f.path)

print(f"Released {len(candidates)} file(s) -> {dst_root}")
