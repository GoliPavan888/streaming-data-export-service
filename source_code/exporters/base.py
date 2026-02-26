from typing import Iterable, List, Tuple

from exporters.utils import MemoryTracker


class BaseExporter:
    format_name = ""
    content_type = ""
    file_extension = ""
    supports_gzip = True

    def stream(self, conn, source_columns: List[str], target_columns: List[str]) -> Iterable[bytes]:
        raise NotImplementedError

    def export_to_file(
        self,
        conn,
        source_columns: List[str],
        target_columns: List[str],
        file_path: str,
        memory_tracker: MemoryTracker | None = None,
    ) -> Tuple[int, float]:
        bytes_written = 0
        with open(file_path, "wb") as handle:
            for chunk in self.stream(conn, source_columns, target_columns):
                handle.write(chunk)
                bytes_written += len(chunk)
                if memory_tracker:
                    memory_tracker.update()
        return bytes_written, memory_tracker.peak_mb if memory_tracker else 0.0
