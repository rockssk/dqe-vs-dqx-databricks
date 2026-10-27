"""Render docs/FAQ.md to a shareable .docx with real tables, headings and monospace code blocks.

Standard library only (writes the OOXML parts directly). Handles the Markdown subset FAQ.md uses:
headings, paragraphs, bullet/numbered lists, tables, fenced code blocks, horizontal rules,
**bold**, *italic*, `code`, [links](url).
Usage: python3 scripts/build_docx.py docs/FAQ.md out.docx [mermaid_fallback.txt]
Word can't render Mermaid, so ```mermaid blocks are replaced by the plain-text fallback when one is given.
"""
import re
import sys
import zipfile
from xml.sax.saxutils import escape

SRC, OUT = sys.argv[1], sys.argv[2]
FALLBACK = open(sys.argv[3], encoding="utf-8").read().splitlines() if len(sys.argv) > 3 else None

DB_RED, DB_DARK, DB_BLUE, CODE_RED, CODE_BG = "FF3621", "1B1B2F", "0078D4", "8B1A10", "F4F4F6"
MONO = "Menlo"
TIGHT = '<w:spacing w:before="0" w:after="0"/>'


# ── inline Markdown → runs ──────────────────────────────────────────────────
def run(text, bold=False, italic=False, code=False, color=None, size=None):
    props = []
    if code:
        props += [f'<w:rFonts w:ascii="{MONO}" w:hAnsi="{MONO}" w:cs="{MONO}"/>', f'<w:color w:val="{CODE_RED}"/>', '<w:sz w:val="18"/>']
    if bold:
        props.append("<w:b/>")
    if italic:
        props.append("<w:i/>")
    if color and not code:
        props.append(f'<w:color w:val="{color}"/>')
    if size and not code:
        props.append(f'<w:sz w:val="{size}"/>')
    rpr = f"<w:rPr>{''.join(props)}</w:rPr>" if props else ""
    return f'<w:r>{rpr}<w:t xml:space="preserve">{escape(text)}</w:t></w:r>'


def runs(text, color=None, size=None, bold=False):
    out = []
    for part in re.split(r"(`[^`]+`)", text):
        if len(part) > 1 and part.startswith("`") and part.endswith("`"):
            out.append(run(part[1:-1], code=True))
            continue
        part = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", part)
        for tok in re.split(r"(\*\*.+?\*\*|(?<![\w*])\*(?!\s).+?(?<!\s)\*(?![\w*]))", part):
            if not tok:
                continue
            if tok.startswith("**") and tok.endswith("**"):
                out.append(run(tok[2:-2], bold=True, color=color, size=size))
            elif tok.startswith("*") and tok.endswith("*") and len(tok) > 2:
                out.append(run(tok[1:-1], italic=True, bold=bold, color=color, size=size))
            else:
                out.append(run(tok, bold=bold, color=color, size=size))
    return "".join(out)


def para(content, style=None, ppr_extra=""):
    st = f'<w:pStyle w:val="{style}"/>' if style else ""
    return f"<w:p><w:pPr>{st}{ppr_extra}</w:pPr>{content}</w:p>"


def table(head, rows):
    border = "".join(f'<w:{s} w:val="single" w:sz="4" w:space="0" w:color="999999"/>' for s in ("top", "left", "bottom", "right", "insideH", "insideV"))
    out = [f'<w:tbl><w:tblPr><w:tblW w:w="5000" w:type="pct"/><w:tblBorders>{border}</w:tblBorders>'
           '<w:tblCellMar><w:top w:w="40" w:type="dxa"/><w:left w:w="80" w:type="dxa"/><w:bottom w:w="40" w:type="dxa"/><w:right w:w="80" w:type="dxa"/></w:tblCellMar></w:tblPr>']
    def cell(text, header):
        shd = f'<w:shd w:val="clear" w:color="auto" w:fill="{DB_DARK}"/>' if header else ""
        body = runs(text, color="FFFFFF", bold=True, size=19) if header else runs(text, size=19)
        return f"<w:tc><w:tcPr>{shd}</w:tcPr>{para(body, ppr_extra=TIGHT)}</w:tc>"
    out.append('<w:tr><w:trPr><w:tblHeader/></w:trPr>' + "".join(cell(h, True) for h in head) + "</w:tr>")
    for r in rows:
        r = (r + [""] * len(head))[: len(head)]
        out.append("<w:tr>" + "".join(cell(c, False) for c in r) + "</w:tr>")
    out.append("</w:tbl>")
    return "".join(out) + para("")


def code_block(lines):
    ppr = (f'<w:shd w:val="clear" w:color="auto" w:fill="{CODE_BG}"/><w:spacing w:before="0" w:after="0" w:line="240" w:lineRule="auto"/>'
           '<w:ind w:left="120" w:right="120"/>')
    rp = f'<w:rPr><w:rFonts w:ascii="{MONO}" w:hAnsi="{MONO}" w:cs="{MONO}" w:eastAsia="{MONO}"/><w:sz w:val="15"/></w:rPr>'
    return "".join(para(f'<w:r>{rp}<w:t xml:space="preserve">{escape(ln) or " "}</w:t></w:r>', ppr_extra=ppr) for ln in lines) + para("")


# ── block Markdown → document body ──────────────────────────────────────────
def cells(row):
    return [c.strip() for c in row.strip().strip("|").split("|")]


lines = open(SRC, encoding="utf-8").read().splitlines()
body, buf, i, num = [], [], 0, 0


def flush():
    global buf
    if buf:
        body.append(para(runs(" ".join(buf))))
        buf = []


while i < len(lines):
    ln = lines[i]
    if ln.startswith("```"):
        flush()
        lang = ln[3:].strip()
        block = []
        i += 1
        while i < len(lines) and not lines[i].startswith("```"):
            block.append(lines[i])
            i += 1
        body.append(code_block(FALLBACK if lang == "mermaid" and FALLBACK else block))
    elif ln.startswith("|") and i + 1 < len(lines) and re.match(r"^\|[\s\-:|]+\|$", lines[i + 1].strip()):
        flush()
        head, rows = cells(ln), []
        i += 2
        while i < len(lines) and lines[i].startswith("|"):
            rows.append(cells(lines[i]))
            i += 1
        body.append(table(head, rows))
        continue
    elif m := re.match(r"^(#{1,3})\s+(.*)", ln):
        flush()
        body.append(para(runs(m.group(2)), style=f"Heading{len(m.group(1))}"))
    elif ln.strip() == "---":
        flush()
        body.append(para("", ppr_extra='<w:pBdr><w:bottom w:val="single" w:sz="6" w:space="1" w:color="CCCCCC"/></w:pBdr>'))
    elif m := re.match(r"^\s*([-*]|\d+\.)\s+(.*)", ln):
        flush()
        if m.group(1)[0].isdigit():
            num += 1
            marker = f"{num}."
        else:
            marker = "•"
        body.append(para(run(marker + "\t") + runs(m.group(2)),
                         ppr_extra='<w:tabs><w:tab w:val="left" w:pos="360"/></w:tabs><w:ind w:left="360" w:hanging="360"/><w:spacing w:after="40"/>'))
    elif not ln.strip():
        flush()
        num = 0
    else:
        buf.append(ln.strip())
    i += 1
flush()

W = 'xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"'
document = (f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><w:document {W}><w:body>{"".join(body)}'
            '<w:sectPr><w:pgSz w:w="12240" w:h="15840"/><w:pgMar w:top="1080" w:right="1080" w:bottom="1080" w:left="1080" w:header="720" w:footer="720" w:gutter="0"/></w:sectPr>'
            "</w:body></w:document>")


def heading_style(level, size, color):
    return (f'<w:style w:type="paragraph" w:styleId="Heading{level}"><w:name w:val="heading {level}"/><w:basedOn w:val="Normal"/><w:next w:val="Normal"/><w:qFormat/>'
            f'<w:pPr><w:keepNext/><w:spacing w:before="{240 if level > 1 else 0}" w:after="80"/><w:outlineLvl w:val="{level - 1}"/></w:pPr>'
            f'<w:rPr><w:b/><w:color w:val="{color}"/><w:sz w:val="{size}"/></w:rPr></w:style>')


styles = (f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><w:styles {W}>'
          '<w:docDefaults><w:rPrDefault><w:rPr><w:rFonts w:ascii="Arial" w:hAnsi="Arial" w:cs="Arial" w:eastAsia="Arial"/>'
          f'<w:sz w:val="21"/><w:color w:val="{DB_DARK}"/></w:rPr></w:rPrDefault><w:pPrDefault><w:pPr><w:spacing w:after="120" w:line="264" w:lineRule="auto"/></w:pPr></w:pPrDefault></w:docDefaults>'
          '<w:style w:type="paragraph" w:default="1" w:styleId="Normal"><w:name w:val="Normal"/><w:qFormat/></w:style>'
          + heading_style(1, 40, DB_RED) + heading_style(2, 30, DB_DARK) + heading_style(3, 24, DB_BLUE) + "</w:styles>")

content_types = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                 '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                 '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
                 '<Default Extension="xml" ContentType="application/xml"/>'
                 '<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
                 '<Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>'
                 "</Types>")
rels = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>'
        "</Relationships>")
doc_rels = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
            "</Relationships>")

with zipfile.ZipFile(OUT, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("[Content_Types].xml", content_types)
    z.writestr("_rels/.rels", rels)
    z.writestr("word/document.xml", document)
    z.writestr("word/styles.xml", styles)
    z.writestr("word/_rels/document.xml.rels", doc_rels)
print("wrote", OUT)
