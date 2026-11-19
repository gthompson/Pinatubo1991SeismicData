"""BOB annual tilt/housekeeping series, separate from seismic amplitude metrics."""
from pathlib import Path
import hashlib,json,re,struct,zipfile
import numpy as np
import pandas as pd

CHANNELS={
 'UBOR':('UBO_','radial_tilt','E-W; inflation west up; X N86W'),
 'UBOT':('UBO_','tangential_tilt','N-S; inflation south up; Y N4E'),
 'UBAT':('UBO_','battery_voltage',''), 'UTMP':('UBO_','sensor_temperature',''),
 'PPOR':('PPO_','radial_tilt','Orientation not supplied'),
 'PPOT':('PPO_','tangential_tilt','Orientation not supplied'),
 'PBAT':('PPO_','battery_voltage',''), 'PTMP':('PPO_','temperature',''),
 'PRAI':('PPO_','rain_related_unconfirmed','Identity inferred from filename only')}

class TILT:
    """Container for signed tilt and supporting series; never filters or detrends.

    Shared RSAM binary layout does not imply RSAM signal semantics. Calendar
    labels are timezone-naive local labels; no additional eight-hour shift.
    """
    def __init__(self,data,metadata):
        self.data=data
        self.metadata=metadata

    @classmethod
    def read(cls,source,*,year=1991,sampling_interval=600):
        if sampling_interval!=600:raise ValueError('This reader validates the supplied 10-minute annual layout only')
        source=Path(source)
        if source.is_file():
            with zipfile.ZipFile(source) as z:
                entries=[(n,z.read(n)) for n in sorted(z.namelist()) if not n.startswith('__MACOSX/') and not n.endswith('/')]
        else:entries=[(str(p.relative_to(source)),p.read_bytes()) for p in sorted(source.rglob('*')) if p.is_file() and p.suffix.upper() in ('.DAT','.TXT')]
        docs={n:b.decode('cp1252') for n,b in entries if n.upper().endswith('.TXT')}
        days=(pd.Timestamp(year+1,1,1)-pd.Timestamp(year,1,1)).days
        expected=days*144;frame=pd.DataFrame({'recorded_local_time':pd.date_range(f'{year}-01-01',periods=expected,freq='600s')})
        meta={'year':year,'sampling_interval_seconds':600,'header_bytes':576,'byte_order':'little',
              'time_basis':'Local time per archived notes; exact phase/date mapping not independently verified. No UTC conversion applied.',
              'units':'Uncalibrated legacy values; channel labels do not establish volts, degrees or microradians.',
              'notes':docs,'channels':{},'reader_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
        for name,blob in entries:
            if not name.upper().endswith('.DAT'):continue
            match=re.fullmatch(r'([A-Z]+)(\d{2})\.DAT',Path(name).name.upper())
            if not match or 1900+int(match[2])!=year:raise ValueError(f'Unexpected filename/year: {name}')
            code=match[1]
            if code in meta['channels']:raise ValueError(f'Duplicate channel: {code}')
            if len(blob)<576 or struct.unpack('<I',blob[:4])[0]!=576:
                raise ValueError(f'{name}: unrecognized binary header')
            if len(blob)!=576+expected*4:raise ValueError(f'{name}: unexpected annual sample count')
            raw=np.frombuffer(blob,dtype='<f4',offset=576).copy()
            missing=(raw==-998)|~np.isfinite(raw)
            frame[code+'_raw']=raw
            frame[code+'_missing']=missing
            station,kind,orientation=CHANNELS.get(code,('unknown','unknown',''))
            valid=np.flatnonzero(~missing)
            meta['channels'][code]={'source_file':name,'sha256':hashlib.sha256(blob).hexdigest(),
                'station':station,'kind':kind,'orientation':orientation,'samples':len(raw),
                'valid_samples':int((~missing).sum()),'missing_samples':int(missing.sum()),
                'first_nonmissing_label':str(frame.recorded_local_time.iloc[valid[0]]) if len(valid) else None,
                'last_nonmissing_label':str(frame.recorded_local_time.iloc[valid[-1]]) if len(valid) else None}
        if not meta['channels']:raise ValueError('No annual TILT files found')
        return cls(frame,meta)

    def plot(self,output=None,*,start=None,end=None):
        """Separate panels and scales per observable; gaps remain gaps."""
        import matplotlib.pyplot as plt
        part=self.data
        if start:part=part[part.recorded_local_time>=pd.Timestamp(start)]
        if end:part=part[part.recorded_local_time<=pd.Timestamp(end)]
        figures={}
        for station in sorted({m['station'] for m in self.metadata['channels'].values()}):
            codes=[c for c,m in self.metadata['channels'].items() if m['station']==station]
            fig,axes=plt.subplots(len(codes),1,figsize=(12,2.1*len(codes)),sharex=True,squeeze=False,layout='constrained')
            for ax,code in zip(axes[:,0],codes):
                values=part[code+'_raw'].where(~part[code+'_missing'])
                ax.plot(part.recorded_local_time,values,lw=.65)
                ax.set_ylabel(code+'\nlegacy units');ax.set_title(self.metadata['channels'][code]['kind'],loc='left',fontsize=9);ax.grid(alpha=.2)
            axes[-1,0].set_xlabel('Recorded local calendar label — phase not independently verified')
            fig.suptitle(station+' — tilt and supporting channels (no calibration or detrending)')
            if output:
                dest=Path(output);dest.mkdir(parents=True,exist_ok=True)
                for ext in ('png','pdf'):fig.savefig(dest/f'{station.strip("_")}_tilt_overview.{ext}',dpi=160)
            figures[station]=fig
        return figures

    def write(self,output):
        output=Path(output);output.mkdir(parents=True,exist_ok=True)
        self.data.to_csv(output/'tilt_records.csv',index=False)
        (output/'tilt_metadata.json').write_text(json.dumps(self.metadata,indent=2)+'\n')
        (output/'README.md').write_text('# BOB TILT compilation\n\nOne full-year row per 10-minute calendar label, with original float values and missing flags per channel. The 576-byte header is excluded, not treated as measurements. −998 is retained in raw columns and masked only for plotting. Time labels follow the conventional annual layout; archived notes state local time. No additional shift, calibration, offset removal, differentiation or temperature compensation is applied. Read tilt_metadata.json for documentary notes, source hashes, orientations and valid coverage. PRAI identity remains unconfirmed.\n')
        return output

def run(source,output):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    obj=TILT.read(source);obj.write(output)
    # Show the occupied period rather than the annual missing-value padding.
    starts=[m['first_nonmissing_label'] for m in obj.metadata['channels'].values() if m['first_nonmissing_label']]
    ends=[m['last_nonmissing_label'] for m in obj.metadata['channels'].values() if m['last_nonmissing_label']]
    figs=obj.plot(output,start=min(starts) if starts else None,end=max(ends) if ends else None)
    for fig in figs.values():plt.close(fig)
    return obj
