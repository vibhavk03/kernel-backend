def build_final_signature(processed_files: dict) -> str:
    parts = []
    for source_name, file_info in sorted(processed_files.items()):
        parts.append(f"{source_name}:{file_info['etag']}")
    return "|".join(parts)
