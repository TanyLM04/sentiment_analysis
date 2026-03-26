import zstandard as zstd
import json
import pandas as pd
from pathlib import Path

def read_zst(filepath):
    """Read a .zst compressed ndjson file line by line."""
    dctx = zstd.ZstdDecompressor(max_window_size=2**31)
    with open(filepath, "rb") as fh:
        with dctx.stream_reader(fh) as reader:
            buffer = ""
            while True:
                chunk = reader.read(2**24)  # 16MB chunks
                if not chunk:
                    break
                buffer += chunk.decode("utf-8", errors="replace")
                lines = buffer.split("\n")
                buffer = lines[-1]  # keep incomplete last line
                for line in lines[:-1]:
                    if line.strip():
                        yield json.loads(line)

def load_subreddit(filepath, dialect_label, file_type="comments"):
    """Load a subreddit dump into a dataframe."""
    records = []
    for obj in read_zst(filepath):
        if file_type == "comments":
            text = obj.get("body", "")
            if text in ("", "[deleted]", "[removed]"):
                continue
            records.append({
                "id":           obj.get("id"),
                "subreddit":    obj.get("subreddit"),
                "dialect":      dialect_label,
                "type":         "comment",
                "text":         text,
                "score":        obj.get("score"),
                "created_utc":  obj.get("created_utc"),
            })
        elif file_type == "submissions":
            title = obj.get("title", "")
            body  = obj.get("selftext", "")
            if body in ("[deleted]", "[removed]"):
                body = ""
            if not title:
                continue
            records.append({
                "id":           obj.get("id"),
                "subreddit":    obj.get("subreddit"),
                "dialect":      dialect_label,
                "type":         "post",
                "text":         (title + " " + body).strip(),
                "score":        obj.get("score"),
                "created_utc":  obj.get("created_utc"),
            })
    return pd.DataFrame(records)


# --- Main ---
SUBREDDIT_DIALECT_MAP = {
    "argentina":    "rioplatense",
    "BuenosAires":  "rioplatense",
    "mexico":       "cdmx",
    "CDMX":         "cdmx",
    "es":           "madrid",
    "Madrid":       "madrid",
}

data_dir = Path("data/pushshift")  # data location
all_dfs = []

for subreddit, dialect in SUBREDDIT_DIALECT_MAP.items():
    for file_type in ["comments", "submissions"]:
        path = data_dir / f"{subreddit}_{file_type}.zst"
        if path.exists():
            print(f"Loading {path.name}...")
            df = load_subreddit(path, dialect, file_type)
            print(f"  -> {len(df)} records")
            all_dfs.append(df)

combined = pd.concat(all_dfs, ignore_index=True)
combined.drop_duplicates(subset="id", inplace=True)
combined.to_csv("data/raw_pushshift.csv", index=False)
print(f"\nTotal records: {len(combined)}")