#!/usr/bin/env python3
"""Extract BOB ZIPs in place, then relocate them to LEGACY/bob_zipfiles_obsolete.

Preview by default. --extract performs extraction and relocation. Existing files
are never overwritten. CRC-failing members are reported and not extracted; their
ZIPs are retained in the obsolete tree for recovery, not discarded.
"""
from pathlib import Path, PurePosixPath
import argparse,hashlib,json,shutil,stat,subprocess,zipfile,zlib
from datetime import datetime,timezone
from pinatubo_steps.config import pipeline_paths


def safe_member(entry):
    name=entry.filename.replace('\\','/')
    parts=PurePosixPath(name)
    if not name or parts.is_absolute() or '..' in parts.parts or ':' in name or '\x00' in name:
        raise ValueError(f'Unsafe member path: {entry.filename!r}')
    if stat.S_ISLNK(entry.external_attr>>16):raise ValueError('Archive symlink refused')
    return name


def no_symlinks(path):
    if any(p.is_symlink() for p in [path,*path.parents]):
        raise ValueError(f'Symlink in destination path: {path}')


def validate_archive(path):
    with zipfile.ZipFile(path) as archive:
        for entry in archive.infolist():safe_member(entry)


def run(config_file=None, *, extract=False):
    paths=pipeline_paths(config_file=config_file)
    source=Path(paths['LEGACY_TOP'])/'BOB'
    obsolete=source.parent/'bob_zipfiles_obsolete'
    report_dir=Path(paths['OUTPUT_ROOT'])/'legacy_bob'/'unpacked_archives'
    if not source.is_dir():raise FileNotFoundError(source)
    unzip=shutil.which('unzip')
    if not unzip:raise RuntimeError('System unzip is required for historical compression')
    archives=sorted(p for p in source.rglob('*') if p.is_file() and p.suffix.lower()=='.zip')
    report=[]
    for archive in archives:
        relative=archive.relative_to(source);archived=obsolete/relative
        row={'archive':str(archive),'relocated_archive':str(archived),'output':str(archive.parent),'status':'planned','members':[]}
        try:
            no_symlinks(archive);no_symlinks(archived)
            if archived.exists():
                row['status']='archive_clash'
                row['details']='Relocation target exists; original ZIP left in place, no extraction attempted'
            else:
                validate_archive(archive)
                if extract:
                    before=hashlib.sha256(archive.read_bytes()).hexdigest();row['source_sha256']=before
                    with zipfile.ZipFile(archive) as contents:
                        for member in contents.infolist():
                            name=safe_member(member);target=archive.parent/name
                            item={'member':member.filename,'destination':str(target)}
                            no_symlinks(target)
                            if target.exists():
                                item['status']='existing_directory' if member.is_dir() and target.is_dir() else 'clash'
                            elif member.is_dir():target.mkdir(parents=True);item['status']='directory_created'
                            else:
                                result=subprocess.run([unzip,'-p',str(archive),member.filename],capture_output=True)
                                payload=result.stdout
                                if result.returncode or len(payload)!=member.file_size or (zlib.crc32(payload)&0xffffffff)!=member.CRC:
                                    item.update(status='damaged',details=result.stderr.decode(errors='replace'))
                                else:
                                    target.parent.mkdir(parents=True,exist_ok=True)
                                    try:
                                        with target.open('xb') as stream:stream.write(payload)
                                        item.update(status='extracted',bytes=len(payload))
                                    except FileExistsError:item['status']='clash'
                            row['members'].append(item)
                    if hashlib.sha256(archive.read_bytes()).hexdigest()!=before:raise RuntimeError('ZIP changed during extraction')
                    archived.parent.mkdir(parents=True,exist_ok=True)
                    # Exclusive destination creation: never replace an existing archive.
                    try:
                        with archive.open('rb') as src,archived.open('xb') as dst:shutil.copyfileobj(src,dst)
                    except FileExistsError:
                        raise RuntimeError('Relocation clash; original ZIP retained')
                    if hashlib.sha256(archived.read_bytes()).hexdigest()!=before:raise RuntimeError('Relocation copy verification failed; original retained')
                    archive.unlink()
                    row['status']='relocated_with_damaged_members' if any(m['status']=='damaged' for m in row['members']) else 'relocated'
        except Exception as exc:row.update(status='failed',details=str(exc))
        report.append(row)
        clashes=sum(m['status']=='clash' for m in row['members'])
        print(f"{row['status']}: {relative} ({clashes} member clashes)")
    if extract:
        report_dir.mkdir(parents=True,exist_ok=True)
        stamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
        report_file=report_dir/f'in_place_extraction_{stamp}.json'
        report_file.write_text(json.dumps(report,indent=2)+'\n');print('Report:',report_file)
    print(f'{len(archives)} ZIP archives; {sum(r["status"] in ("failed","archive_clash","relocated_with_damaged_members") for r in report)} requiring review.')
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',default=None,help='Shared conversion_config.json override')
    parser.add_argument('--extract',action='store_true',help='Extract in place AND relocate ZIPs; otherwise preview only')
    args=parser.parse_args();report=run(args.config,extract=args.extract)
    if any(r['status'] in ('failed','archive_clash','relocated_with_damaged_members') for r in report):raise SystemExit(1)
if __name__=='__main__':main()
