"""Empaqueta el código y el esquema SQL para --py-files de Spark."""

import argparse
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="Ruta del archivo ZIP de salida")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    source = root / "src"
    paths = [*source.rglob("*.py"), source / "serving" / "schema.sql"]

    with ZipFile(args.output, mode="w", compression=ZIP_DEFLATED) as archive:
        # src es un paquete de espacio de nombres y zipimport necesita su entrada.
        archive.writestr("src/", "")
        for path in sorted(paths):
            archive.write(path, path.relative_to(root).as_posix())


if __name__ == "__main__":
    main()
