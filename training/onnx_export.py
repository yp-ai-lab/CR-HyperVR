from __future__ import annotations

from pathlib import Path
from sentence_transformers import SentenceTransformer


def export_onnx(model_dir: str = "models/movie-minilm-v1", onnx_out: str = "models/movie-minilm-v1/model.onnx") -> None:
    model = SentenceTransformer(model_dir)
    Path(Path(onnx_out).parent).mkdir(parents=True, exist_ok=True)
    model.export(
        export_path=onnx_out,
        format="onnx",
        quantize=False,
        optimize=True,
    )
    print("Exported ONNX to", onnx_out)


if __name__ == "__main__":
    export_onnx()

