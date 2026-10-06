"""Secure, in-memory XML package expansion and repository merge."""
from __future__ import annotations
from copy import deepcopy
from io import BytesIO
from pathlib import PurePosixPath
from zipfile import BadZipFile, ZipFile
from defusedxml import ElementTree as SafeET
from xml.etree import ElementTree as BuildET

def expand_uploads(uploads, *, allow_zip, max_files, max_file_bytes, max_total_bytes):
    result=[]; seen={}; total=0
    def add(name, content):
        nonlocal total
        safe=PurePosixPath(name.replace("\\","/")).name
        if not safe.lower().endswith(".xml"): raise ValueError(f"Only XML files are allowed: {name}")
        if not content: raise ValueError(f"XML file is empty: {safe}")
        if len(content)>max_file_bytes: raise ValueError(f"XML file exceeds size limit: {safe}")
        total += len(content)
        if total>max_total_bytes: raise ValueError("XML package exceeds total size limit.")
        key=safe.casefold()
        if key in seen:
            if seen[key] != content: raise ValueError(f"Conflicting duplicate filename: {safe}")
            return
        seen[key]=content; result.append((safe,content))
        if len(result)>max_files: raise ValueError("XML package exceeds file-count limit.")
    for name,content in uploads:
        safe=PurePosixPath((name or "upload.xml").replace("\\","/")).name
        if safe.lower().endswith(".xml"): add(safe,content); continue
        if not safe.lower().endswith(".zip"): raise ValueError(f"Only .xml and .zip are accepted: {safe}")
        if not allow_zip: raise ValueError("ZIP uploads are disabled.")
        try:
            with ZipFile(BytesIO(content)) as z:
                for info in z.infolist():
                    if info.is_dir(): continue
                    path=PurePosixPath(info.filename.replace("\\","/"))
                    if path.is_absolute() or ".." in path.parts: raise ValueError(f"Unsafe ZIP path: {info.filename}")
                    if path.suffix.lower() != ".xml": raise ValueError(f"Non-XML ZIP member: {info.filename}")
                    add(path.name,z.read(info))
        except BadZipFile as e: raise ValueError(f"Invalid ZIP package: {safe}") from e
    if not result: raise ValueError("At least one XML file is required.")
    return result

def merge_xml_files(files):
    root=BuildET.Element("POWERMART"); repository=BuildET.SubElement(root,"REPOSITORY"); folders={}
    for name,content in files:
        try: parsed=SafeET.parse(BytesIO(content)).getroot()
        except Exception as e: raise ValueError(f"Invalid Informatica XML {name}: {e}") from e
        for folder in [x for x in parsed.iter() if str(x.tag).split("}")[-1].upper()=="FOLDER"]:
            fname=folder.attrib.get("NAME","UNKNOWN")
            target=folders.get(fname)
            if target is None:
                target=BuildET.SubElement(repository,"FOLDER",dict(folder.attrib)); folders[fname]=target
            for child in folder: target.append(deepcopy(child))
    if not folders: raise ValueError("No Informatica FOLDER elements found.")
    return BuildET.tostring(root,encoding="utf-8",xml_declaration=True)

def object_summary(content):
    root=SafeET.parse(BytesIO(content)).getroot(); counts={"mappings":0,"sessions":0,"workflows":0,"worklets":0}
    keys={"MAPPING":"mappings","SESSION":"sessions","WORKFLOW":"workflows","WORKLET":"worklets"}
    for x in root.iter():
        k=keys.get(str(x.tag).split("}")[-1].upper())
        if k: counts[k]+=1
    return counts
