# imghdr.py
# Fallback shim for systems where stdlib imghdr might be missing.
# If Pillow (PIL) is installed it will use it. Otherwise provides minimal safe behavior.

try:
    # first try to import the stdlib module (if available on system)
    import importlib
    _imghdr = importlib.import_module('imghdr')
    # Re-export functions we need
    what = getattr(_imghdr, 'what', None)
    tests = getattr(_imghdr, 'tests', None)
except Exception:
    # Provide simple fallback
    try:
        from PIL import Image
    except Exception:
        Image = None

    def what(h, hname=None):
        """
        Minimal detection:
        - If PIL available, attempt to open buffer
        - else return None
        """
        if hname:
            try:
                with open(hname, 'rb') as f:
                    data = f.read(512)
            except Exception:
                return None
        else:
            data = h if isinstance(h, (bytes, bytearray)) else None

        if Image is not None and data is not None:
            from io import BytesIO
            try:
                img = Image.open(BytesIO(data))
                fmt = img.format
                if fmt:
                    return fmt.lower()
            except Exception:
                return None
        return None

    tests = []

# Export names
__all__ = ['what', 'tests']
