from pathlib import Path


# to see https://www.iana.org/assignments/media-types/media-types.xhtml
def get_content_type(file_path: Path):
  ext_name = file_path.suffix.lower()
  if ext_name == ".pdf":
    return "application/pdf"
  else:
    return "application/octet-stream"