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
    openpyxl drops embedded images, drawings, and their content-type entries
    when loading/saving an xlsx.  This function re-injects them from the
    original template so the output is valid and opens without a repair prompt.

    Strategy:
      - [Content_Types].xml : use the template's copy as the base; merge in
        any Override entries that openpyxl added (e.g. sharedStrings) that
        aren't already in the template.
      - xl/worksheets/_rels/ : restore from template (drawing relationship).
      - xl/drawings/         : restore from template (drawing XML + image rels).
      - xl/media/            : restore from template (the actual image bytes).
      - worksheet XML        : re-insert <drawing r:id="..."/> before </worksheet>
        and ensure xmlns:r is declared on the root element.
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

        # Files to restore from the template
        inject = {
            n for n in src_files - opx_files
            if any(n.startswith(p) for p in ASSET_PREFIXES)
            or n.startswith(RELS_PREFIX)
        }

        # ── Merge [Content_Types].xml ─────────────────────────────────────────
        src_ct = src.read("[Content_Types].xml").decode()
        opx_ct = opx.read("[Content_Types].xml").decode()

        # Collect PartNames already declared in the template
        src_parts = set(re.findall(r'PartName="([^"]+)"', src_ct))

        # Any Override in openpyxl output not in template (e.g. sharedStrings)
        extra_overrides = []
        for m in re.finditer(r'<Override\b[^>]*/>', opx_ct):
            part = re.search(r'PartName="([^"]+)"', m.group(0))
            if part and part.group(1) not in src_parts:
                extra_overrides.append(m.group(0))

        merged_ct = src_ct.replace(
            "</Types>",
            "".join(extra_overrides) + "</Types>"
        )

        # ── Build drawing-tag map from worksheet rels ─────────────────────────
        # { "xl/worksheets/sheet1.xml": '<drawing r:id="rId1"/>' }
        drawing_refs = {}
        for rels_name in inject:
            if not rels_name.startswith(RELS_PREFIX):
                continue
            rels_xml = src.read(rels_name).decode()
            # Attribute order varies — match either Id-first or Type-first
            drawing_ids = re.findall(
                r'<Relationship\b[^>]*\bId="([^"]+)"[^>]*\bType="[^"]*drawing[^"]*"',
                rels_xml,
            )
            if not drawing_ids:
                drawing_ids = re.findall(
                    r'<Relationship\b[^>]*\bType="[^"]*drawing[^"]*"[^>]*\bId="([^"]+)"',
                    rels_xml,
                )
            if drawing_ids:
                # "xl/worksheets/_rels/sheet1.xml.rels" → "xl/worksheets/sheet1.xml"
                sheet_name = re.sub(r"/_rels/(.+)\.rels$", r"/\1", rels_name)
                drawing_refs[sheet_name] = "".join(
                    f'<drawing r:id="{rid}"/>' for rid in drawing_ids
                )

        # ── Write output zip ──────────────────────────────────────────────────
        for name in opx.namelist():
            if name == "[Content_Types].xml":
                out.writestr(name, merged_ct.encode())
            elif name in drawing_refs:
                content = opx.read(name).decode()
                tags = drawing_refs[name]
                if "<drawing " not in content:
                    # Ensure xmlns:r is declared on the root <worksheet> element
                    content = re.sub(
                        r'(<worksheet\b)([^>]*>)',
                        lambda m: m.group(1) + m.group(2)
                        if 'xmlns:r=' in m.group(2)
                        else m.group(1)
                        + ' xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"'
                        + m.group(2),
                        content,
                        count=1,
                    )
                    content = content.replace("</worksheet>", tags + "</worksheet>")
                out.writestr(name, content.encode())
            else:
                out.writestr(name, opx.read(name))

        # Restore asset files from template
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
