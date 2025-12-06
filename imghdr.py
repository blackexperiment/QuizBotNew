# imghdr.py -- lightweight shim for Python versions missing stdlib imghdr
# Detect common image types by header bytes.
# Provides: what(filename, h=None)

from typing import Optional, Union

def _read_header(fname_or_fp, n=32):
    # accepts filename (str/path) or file-like
    if hasattr(fname_or_fp, "read"):
        pos = None
        try:
            pos = fname_or_fp.tell()
        except Exception:
            pos = None
        b = fname_or_fp.read(n)
        try:
            if pos is not None:
                fname_or_fp.seek(pos)
        except Exception:
            pass
        return b
    else:
        with open(fname_or_fp, "rb") as f:
            return f.read(n)

def what(file: Union[str, object], h: Optional[bytes]=None) -> Optional[str]:
    """
    Return a string describing the image type, or None.
    Mirrors the stdlib imghdr.what minimal behaviour for common types.
    """
    if h is None:
        try:
            h = _read_header(file, 64)
        except Exception:
            return None
    if not h or len(h) == 0:
        return None

    # GIF
    if h[:6] in (b"GIF87a", b"GIF89a"):
        return "gif"
    # PNG
    if h.startswith(b"\211PNG\r\n\032\n"):
        return "png"
    # JPEG
    if h.startswith(b"\xff\xd8\xff"):
        return "jpeg"
    # TIFF little endian II*\x00 or big endian MM\x00*
    if h[:4] in (b"II*\x00", b"MM\x00*"):
        return "tiff"
    # BMP
    if h.startswith(b"BM"):
        return "bmp"
    # WEBP: RIFF....WEBP
    if h[:4] == b"RIFF" and b"WEBP" in h[8:16]:
        return "webp"
    # ICO (starts with 0,0,1,0)
    if len(h) >= 4 and h[0:4] == b"\x00\x00\x01\x00":
        return "ico"
    # SVG: ASCII <?xml or <svg
    try:
        txt = h.decode("utf-8", errors="ignore").lstrip()
        if txt.startswith("<svg") or txt.startswith("<?xml"):
            return "svg"
    except Exception:
        pass

    return None

# compatibility names
__all__ = ["what"]
