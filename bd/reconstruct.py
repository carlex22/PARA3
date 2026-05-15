#!/usr/bin/env python3
from pathlib import Path
chunks_dir = Path(__file__).parent
output = chunks_dir / "chroma_db.tar.gz"
chunks = sorted(chunks_dir.glob("chroma_db.tar.gz.part*"))
with open(output, "wb") as out:
    for c in chunks: out.write(c.read_bytes())
print(f"✅ {output}
Descompacte: tar -xzf chroma_db.tar.gz")