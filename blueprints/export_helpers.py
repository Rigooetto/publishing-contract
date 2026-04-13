"""
Shared utilities for Excel report generation.

- stitch_xlsx_assets: re-inject images/drawings stripped by openpyxl
- open_xls_template:  load an .xls template preserving formatting (xlutils)
"""
import io
import re
import zipfile


def stitch_xlsx_assets(template_path, opx_bytes):
    """
    openpyxl drops embedded images and drawings when loading/saving an xlsx.
    This function re-injects them from the original template zip so the
    output file is identical to the template except for the data rows.

    template_path : str   — path to the original .xlsx template file
    opx_bytes     : bytes — output of wb.save() via openpyxl
    Returns       : bytes — fixed xlsx with assets restored
    """
    with open(template_path, "rb") as f:
        template_bytes = f.read()

    ASSET_PREFIXES = ("xl/media/", "xl/drawings/")
    RELS_PREFIX    = "xl/worksheets/_rels/"

    result = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(template_bytes)) as src, \
         zipfile.ZipFile(io.BytesIO(opx_bytes)) as opx, \
         zipfile.ZipFile(result, "w", zipfile.ZIP_DEFLATED) as out:

        opx_files = set(opx.namelist())
        src_files = set(src.namelist())

        # Files in template not in openpyxl output that we want to restore
        inject = {
            n for n in src_files - opx_files
            if any(n.startswith(p) for p in ASSET_PREFIXES)
            or n.startswith(RELS_PREFIX)
        }

        src_ct = src.read("[Content_Types].xml").decode()

        for name in opx.namelist():
            data = opx.read(name)
            if name == "[Content_Types].xml" and inject:
                content = data.decode()
                # Re-inject missing Default entries (e.g. jpeg)
                for m in re.finditer(r'<Default Extension="(?!rels|xml)[^"]*"[^/]*/>', src_ct):
                    tag = m.group(0)
                    ext = re.search(r'Extension="([^"]+)"', tag).group(1)
                    if ext not in content:
                        content = content.replace("</Types>", tag + "</Types>")
                # Re-inject Override entries for drawings
                for n in inject:
                    if n.startswith("xl/drawings/") and n.endswith(".xml"):
                        part = "/" + n
                        if part not in content:
                            m2 = re.search(
                                r'<Override[^>]+' + re.escape(part) + r'[^/]*/>', src_ct
                            )
                            if m2:
                                content = content.replace("</Types>", m2.group(0) + "</Types>")
                data = content.encode()
            out.writestr(name, data)

        # Inject asset files from template
        for name in inject:
            out.writestr(name, src.read(name))

    result.seek(0)
    return result.read()


def open_xls_template(template_path):
    """
    Load an .xls template preserving all cell formatting using xlutils.
    Returns an xlwt Workbook ready to have rows appended.
    Use wb.get_sheet(0) to access the first sheet.
    """
    import xlrd
    from xlutils.copy import copy as xl_copy
    rb = xlrd.open_workbook(template_path, formatting_info=True)
    return xl_copy(rb)
