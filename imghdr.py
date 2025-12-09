# imghdr.py (shim)
try:
    import importlib
    _imghdr = importlib.import_module('imghdr')
    what = getattr(_imghdr, 'what', None)
    tests = getattr(_imghdr, 'tests', None)
except Exception:
    try:
        from PIL import Image
    except Exception:
        Image = None
    def what(h, hname=None):
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
__all__ = ['what', 'tests']
