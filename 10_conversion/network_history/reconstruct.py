"""Evidence-based, provisional Pinatubo network reconstruction.
Run with python -m network_history.reconstruct; no archive data are modified.
"""
from pathlib import Path
import os,json,copy,csv,re,tempfile,sys
BASE=Path(__file__).resolve().parent
os.environ.setdefault('MPLCONFIGDIR',str(Path(tempfile.gettempdir())/'pinatubo-plot-cache/matplotlib'))
os.environ.setdefault('GMT_USERDIR',str(Path(tempfile.gettempdir())/'pinatubo-plot-cache/gmt'))
os.environ.setdefault('GMT_SESSIONDIR',str(Path(tempfile.gettempdir())/'pinatubo-plot-cache/gmt/sessions'))
Path(os.environ['GMT_SESSIONDIR']).mkdir(parents=True,exist_ok=True)
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch,FancyArrowPatch
from obspy import UTCDateTime,read_inventory
from obspy.core.inventory import Inventory,Network,Station,Channel,Site,Comment,Equipment
from obspy.core.inventory.response import Response,PolesZerosResponseStage,ResponseStage,InstrumentSensitivity
DATA=BASE/'data'
def load(name):return json.loads((DATA/name).read_text())
def epochs(header_data_dir=None):
    rows=[]
    def add(sta,start,end=None,sensor='L4-C',att=None,ch='EHZ',depth=1.,note='Date-only boundaries use nominal UTC midnight; field clock/time basis uncertain.',rate=100.):
        rows.append(dict(station=sta,channel=ch,start=start,end=end,sensor=sensor,attenuation_db=att,depth_m=depth,sample_rate_hz=rate,note=note))
    add('LIL','1991-04-25','1991-05-01',att=30)
    add('PIE','1991-04-30','1991-06-16',att=18,note='Destroyed June 15; end June 16 is an inclusive-day convention, not measured failure time.')
    add('UBO','1991-05-01','1991-06-11T14:00:00',att=36,depth=0,note='Buried in rocky fill, exact depth unknown: 0 m placeholder. Seismic lost June 11 22:00 local; nominal UTC conversion, clock uncertainty retained.')
    add('BUR','1991-05-05','1991-06-12',att=24,note='Link lost June 11; end is inclusive-day convention.')
    add('PPO','1991-05-03','1991-05-12',att=36,depth=0)
    add('PPO','1991-05-12','1991-05-30',sensor='L22-D',att=24,depth=0)
    add('PPO','1991-05-30','1991-06-12',sensor='L22-D',att=30,depth=0)
    for ch in ['EHN','EHE']:add('PPO','1991-05-12','1991-06-12',sensor='L22-D',att=30,ch=ch,depth=0)
    add('BUG','1991-05-11','1991-06-16',att=18,note='Lightning outage and May 28 reinstall in chronology; exact outage onset unknown. June 15 heat damage; inclusive-day end.')
    add('GRN','1991-05-14','1991-06-16',att=24,depth=0,note='24 dB inferred retrospectively from pulses. Reburied 1.5 m in early June; exact date unknown, initial depth unknown; 0 m placeholder.')
    add('CAB','1991-05-01','1991-05-05T04:45:00',att=30,depth=0,note='Buried sensor; depth unreported, 0 m placeholder. Date-only start conventional UTC.')
    add('CAB','1991-05-05T04:45:00','1991-06-16',att=24,depth=0,note='Unknown burial depth: 0 m placeholder. 12:45 local attenuation change converted nominally to UTC. Battery outage separate. Later calibration continuity unverified.')
    headers=pd.read_csv(Path(header_data_dir or DATA)/'waveform_header_presence.csv',dtype={'file_month':str})
    post=headers[headers.file_month.isin(['07','08'])]
    # First identities in late June are observational bounds, never deployment dates.
    for (sta,ch),group in post.groupby(['station','channel']):
        candidates=headers[(headers.station==sta)&(headers.channel==ch)&(headers.date>='1991-06-16')]
        first=candidates['first'].min()
        if sta in ['CAB','BUG']:first='1991-06-16' # conventional continuity/rebuild boundary, explicitly uncertain
        add(sta,first,sensor='unknown',ch=ch,att=None,depth=0,note='Post-eruption configuration unknown. Start is first surviving header identity (CAB/BUG: conventional June 16 boundary); not verified installation or calibration. Depth 0 is an unknown-depth placeholder.')
    add('PI2',None,sensor='unknown',ch='ELZ',depth=0,note='Undated low-gain identity PI2L in station list. No verified epoch or response; earlier conversion may have collapsed it into EHZ.')
    for sta,start,end in [('UBO','1991-05-31','1991-06-14'),('PPO','1991-06-02','1991-06-12')]:
        for ch in ['WA1','WA2']:add(sta,start,end,sensor='electronic tilt, model unverified',ch=ch,depth=0,rate=1/600,note='Draft radial/tangential tilt codes, not geographic N/E. UBO axes 274/4 degrees; sign convention west-up/south-up needs raw-data verification. PPO floor installation unreliable; June 10 improvement incomplete. End boundaries approximate; no calibration supplied.')
    return rows

def nominal_response(sensor,att):
    cfg=load('response_assumptions.json')['sensors'][sensor]; w=2*np.pi*cfg['natural_frequency_hz']; z=cfg['damping'];f=5.
    poles=[complex(-z*w,w*np.sqrt(1-z*z)),complex(-z*w,-w*np.sqrt(1-z*z))]
    s=2j*np.pi*f;shape=abs(s*s/((s-poles[0])*(s-poles[1])));gain=cfg['sensitivity_v_per_mps']*shape
    stages=[PolesZerosResponseStage(1,gain,f,'M/S','V','LAPLACE (RADIANS/SECOND)',f,[0j,0j],poles,normalization_factor=1/shape)]
    if att is not None:stages.append(ResponseStage(2,10**(-att/20),f,'V','V'))
    return Response(instrument_sensitivity=InstrumentSensitivity(gain*(10**(-att/20) if att is not None else 1),f,'M/S','V'),response_stages=stages)

def inventory(rows,models=False):
    stations=[]
    for info in load('stations.json'):
        elev=info['elevation_m']
        if elev is None:
            import xarray as xr
            with xr.open_dataarray(DATA/'modern_earth_relief.nc', engine='scipy') as grid:elev=float(grid.interp(lon=info['longitude'],lat=info['latitude']))
        comments=[Comment('DRAFT: historical datum unknown; plotting assumes geographic WGS84. '+info['coordinate_source']),Comment('Alternative coordinates: '+json.dumps(info['alternatives'])),Comment('Unknown electronics: VCO/preamp, discriminator/filter and ADC scale. No complete counts-to-ground-motion response supplied.')]
        if info['elevation_m'] is None:comments.append(Comment('Required elevation field estimated from MODERN 15-arcsec DEM; not a historical measurement.'))
        st=Station(info['code'],info['latitude'],info['longitude'],elev,site=Site(name=info['name']),comments=comments)
        for row in [r for r in rows if r['station']==info['code']]:
            ch=row['channel']; az={'Z':0,'N':0,'E':90}.get(ch[-1]);dip=-90 if ch.endswith('Z') else 0
            if ch.startswith('WA'):az=({'WA1':274,'WA2':4}[ch] if info['code']=='UBO' else None)
            c=Channel(ch,'',info['latitude'],info['longitude'],elev,row['depth_m'],azimuth=az,dip=dip,sample_rate=row['sample_rate_hz'],start_date=UTCDateTime(row['start']) if row['start'] else None,end_date=UTCDateTime(row['end']) if row['end'] else None,comments=[Comment(row['note']),Comment('Attenuation dB: '+str(row['attenuation_db'])+'; sensor: '+row['sensor']+'; electronics scale UNKNOWN')],sensor=Equipment(type=row['sensor'],description='Provisional historical identification; no serial-specific calibration.'))
            if models and row['sensor'] in ['L4-C','L22-D']:
                c.response=nominal_response(row['sensor'],row['attenuation_db']);c.comments.append(Comment('NOMINAL VOLTAGE MODEL ONLY: output V, not digital counts. Sensor coil/shunt assumed; electronics omitted.'))
            st.channels.append(c)
        stations.append(st)
    return Inventory([Network('XB',stations=stations,description='Provisional 1991 Pinatubo network, including undated and auxiliary channels')],source='Pinatubo evidence reconstruction — DRAFT; unknown electronics explicit')

def maps(out):
    import xarray as xr
    from matplotlib.colors import LightSource
    with xr.open_dataarray(DATA/'modern_earth_relief.nc', engine='scipy') as g:grid=g.load()
    sites={s['code']:s for s in load('stations.json')}
    for period,codes in [('early_june',['BUG','BUR','CAB','GRN','PIE','PPO','UBO']),('july_august',['BUG','CAB','CRW','DON','FNG','PI2','QAD'])]:
        fig,ax=plt.subplots(figsize=(9,9));extent=[float(grid.lon.min()),float(grid.lon.max()),float(grid.lat.min()),float(grid.lat.max())]
        rgb=LightSource(315,45).shade(grid.values,cmap=plt.get_cmap('gist_earth'),vmin=0,vmax=1800,vert_exag=.3,blend_mode='soft')
        ax.imshow(rgb,extent=extent,origin='lower',alpha=.8,interpolation='bilinear');ax.contour(grid.lon,grid.lat,grid.values,levels=[500,1000],colors='k',alpha=.2,linewidths=.5)
        offsets={'CAB':(8,-16),'PI2':(8,7),'PIE':(8,7),'PPO':(-35,9),'UBO':(-35,-15),'GRN':(-35,0),'BUG':(-35,-12)}
        for code in codes:
            s=sites[code];ax.scatter(s['longitude'],s['latitude'],marker='^',s=95,color='#b41e29',edgecolor='white',zorder=4);ax.annotate(code,(s['longitude'],s['latitude']),xytext=offsets.get(code,(8,8)),textcoords='offset points',fontweight='bold',fontsize=11,bbox=dict(facecolor='white',alpha=.75,edgecolor='none',pad=1))
        # Approximate summit reference only; not a recovered station coordinate.
        ax.scatter(120.35,15.13,marker='*',s=160,color='black');ax.annotate('Pinatubo\n(approx.)',(120.35,15.13),xytext=(10,0),textcoords='offset points',fontsize=8)
        x,y=120.22,15.015;dx=5/(111.32*np.cos(np.deg2rad(y)));ax.plot([x,x+dx],[y,y],color='k',lw=3);ax.text(x+dx/2,y+.006,'5 km',ha='center')
        ax.set(xlim=(120.19,120.60),ylim=(14.99,15.44),xlabel='Longitude °E',ylabel='Latitude °N',title='Pinatubo seismic network — '+('early June 1991' if period=='early_june' else 'July–August 1991'))
        ax.set_aspect(1/np.cos(np.deg2rad(15.2)));ax.grid(alpha=.15)
        fig.text(.11,.035,'Modern 15-arcsec Earth relief: not the 1991 surface. Historical coordinate datum unresolved.\nPVO/repeater positions unverified and unplotted. '+('Outages and June 11 losses are in the chronology.' if period=='early_june' else 'July/August union of header identities; continuous operation and deployment dates unverified.'),fontsize=8)
        fig.subplots_adjust(bottom=.18)
        from matplotlib.cm import ScalarMappable
        from matplotlib.colors import Normalize
        cb=fig.colorbar(ScalarMappable(norm=Normalize(0,1800),cmap='gist_earth'),ax=ax,orientation='horizontal',fraction=.035,pad=.10,aspect=35)
        cb.set_label('Modern elevation (m); shading modifies colours',fontsize=8);cb.ax.tick_params(labelsize=8)
        fig.savefig(out/f'network_{period}.png',dpi=200);fig.savefig(out/f'network_{period}.svg');plt.close(fig)

def diagram(out,name,title,nodes,edges,footer):
    fig,ax=plt.subplots(figsize=(13,8));ax.set(xlim=(0,13),ylim=(0,8));ax.axis('off');ax.set_title(title,fontsize=15,pad=16)
    for key,(x,y,label) in nodes.items():
        ax.add_patch(FancyBboxPatch((x-.95,y-.36),1.9,.72,boxstyle='round,pad=.06',facecolor='#e7f0f7',edgecolor='#315a73',zorder=2));ax.text(x,y,label,ha='center',va='center',fontsize=8,zorder=3)
    records=[]
    for a,b,label in edges:
        x,y,_=nodes[a];xx,yy,_=nodes[b];dx,dy=xx-x,yy-y
        t=min(1.07/abs(dx) if dx else float('inf'),.47/abs(dy) if dy else float('inf'))
        ax.add_patch(FancyArrowPatch((x+t*dx,y+t*dy),(xx-t*dx,yy-t*dy),arrowstyle='-|>',mutation_scale=12,shrinkA=0,shrinkB=0,color='#567080',zorder=1));ax.text((x+xx)/2,(y+yy)/2+.13,label,fontsize=7,ha='center',bbox=dict(facecolor='white',edgecolor='none',alpha=.9,pad=1))
        records.append(dict(source=a,target=b,link=label))
    fig.text(.06,.025,footer,fontsize=8);fig.savefig(out/f'{name}.png',dpi=180,bbox_inches='tight');fig.savefig(out/f'{name}.svg',bbox_inches='tight');plt.close(fig);pd.DataFrame(records).to_csv(out/f'{name}_edges.csv',index=False)

def diagrams(out):
    nodes={'BUG':(1,6.8,'BUG'),'GRN':(4,6.8,'GRN + repeater'),'PIE':(4,5.3,'PIE'),'UBO':(4,4.1,'UBO seismic + tilt\nseparate radio paths'),'CAB':(4,2.8,'CAB'),'BUR':(1,1.1,'BUR'),'PPO':(4,1.1,'PPO Z/N/E + BUR\nLDR: tilt / rain'),'BH':(8,1.1,'Bunker Hill\nradio / microwave'),'PVO':(11,4.1,'PVO at Clark\nreceivers / discriminators')}
    edges=[('BUG','GRN','radio'),('GRN','PVO','radio'),('PIE','PVO','radio'),('UBO','PVO','radio / LDR'),('CAB','PVO','radio'),('BUR','PPO','radio'),('PPO','BH','multiplex radio'),('BH','PVO','military microwave')]
    diagram(out,'telemetry','Documented pre-eruption telemetry — schematic, not geographic',nodes,edges,'Lockhart / Murray. Bunker Hill link lost June 11. June 14 Crow’s Nest attempt failed; not an operating route.\nRebuilt July/August routes are unresolved; no post-eruption links inferred from station coordinates.')
    nodes={'analog':(1.2,6.8,'Radio discriminators\nanalog station signals'),'drum':(4.1,6.8,'Helicorders\nseparate clock domain'),'willie':(4.1,5.3,'WILLIE IBM-AT\nMDETECT + SSAM'),'rick':(1.3,3.9,'RICKY IBM-XT\nlahar acquisition'),'sleep':(1.3,2.5,'SLEEPY IBM-XT\ntilt acquisition'),'rsam':(1.3,1.1,'Tandy Model 100\nRSAM acquisition'),'share':(4.3,2.5,'Buffered serial\nprintsharer'),'pork':(7.2,2.5,'PORKY IBM-AT\naggregate / graphics'),'lan':(8,5.3,'2 Mb/s peer LAN'),'squab':(11.4,6.8,'SQUAB 386\nPCEQ / HYPO71PC / tape'),'dave':(11.4,5.3,'DAVE / ANDY\nanalysis / communications'),'remote':(11.4,2.5,'ANDY: 9600-baud modem\nRemote → CVO / AVO')}
    edges=[('analog','drum','analog'),('analog','willie','100 sps / 12-bit'),('willie','share','SSAM: 1 min'),('rick','share','when received'),('sleep','share','10 min'),('rsam','share','10 min'),('share','pork','serial'),('willie','lan','event waveforms'),('pork','lan','shared products'),('lan','squab','events'),('lan','dave','files'),('dave','remote','communications')]
    diagram(out,'pvo_operations','PVO operations systems — acquisition clocks remain independent',nodes,edges,'Murray: initial 1991 configuration; later software improvements are not assumed present from the start.\nAggregation and LAN transfer do not synchronize clocks. Cadences describe products/transfers, not all native sampling rates.')

def build_all(output_dir=None, *, header_data_dir=None):
    project = BASE.parents[1]
    if str(project) not in sys.path:
        sys.path.insert(0, str(project))
    from pinatubo_config import external_output_root
    if output_dir is None:
        from pinatubo_steps.config import pipeline_paths
        output_dir = pipeline_paths()['NETWORK_HISTORY_OUTPUT']
    out=external_output_root(output_dir, relative_to=BASE)
    out.mkdir(parents=True,exist_ok=True)
    rows=epochs(header_data_dir=header_data_dir);pd.DataFrame(rows).to_csv(out/'pinatubo_channel_epochs.csv',index=False)
    stationdata=load('stations.json');pd.DataFrame([{k:v for k,v in s.items() if k!='alternatives'} for s in stationdata]).to_csv(out/'pinatubo_stations.csv',index=False)
    from pyproj import Geod
    geod=Geod(ellps='WGS84')
    alternatives=[dict(station=s['code'],**a,horizontal_distance_from_selected_m=geod.inv(s['longitude'],s['latitude'],a['longitude'],a['latitude'])[2],elevation_difference_from_selected_m=(a['elevation_m']-s['elevation_m'] if a['elevation_m'] is not None and s['elevation_m'] is not None else None)) for s in stationdata for a in s['alternatives']]
    pd.DataFrame(alternatives).to_csv(out/'alternative_coordinates.csv',index=False)
    hist=load('history_evidence.json')
    def chronological(r):
        date=r['date_as_reported']; match=re.search(r'1991-\d\d(?:-\d\d)?',date)
        return match.group(0) if match else {'early June 1991':'1991-06-01','latter May and early June 1991':'1991-05-20','early July 1991':'1991-07-01','early April 1991':'1991-04-01'}.get(date,'9999')
    hist.sort(key=chronological)
    pd.DataFrame(hist).to_csv(out/'pinatubo_deployment_history.csv',index=False)
    clocks=load('timing_evidence.json');pd.DataFrame(clocks).to_csv(out/'timing_evidence.csv',index=False)
    inventory(rows).write(str(out/'pinatubo_provisional.xml'),format='STATIONXML',validate=True)
    inventory(rows,True).write(str(out/'pinatubo_nominal_voltage_models.xml'),format='STATIONXML',validate=True)
    text=['# Pinatubo network chronology — evidence draft','Every change currently documented in the reviewed sources is retained below. Absence of an entry is not evidence of no change. Date precision and source clock basis must be preserved; coordinate discrepancies do not establish relocations.','']
    for r in hist:text.append(f"- **{r['date_as_reported']} — {r['station']}: {r['event']}**. {r['detail']} Precision: {r.get('date_precision','unspecified')}. Source: {r.get('source','unspecified')}.")
    text+=['','## Archive observations','July/August headers contain BUG, CAB, CRW, DON, FNG, PI2 and QAD; CAB includes N and Z. These establish recorded identities, not installation dates or valid signals. Old BUR/GRN/PIE/PPO/UBO names persist in some late-June headers after documented failures. Stale configuration, placeholders and timing errors remain hypotheses requiring waveform inspection. PI2L may have been collapsed into EHZ by the previous converter.','', 'Rain/lahar acquisition and remote platform coordinates, calibration and installation dates remain incomplete. BOB/STATIONS.TXT distinguishes LSAC, USAC, MARL and ABAC; these are not invented as georeferenced stations.']
    text+=['','Primary references: [Lockhart](https://pubs.usgs.gov/pinatubo/lock/), [Murray](https://pubs.usgs.gov/pinatubo/murray/), [Ewert](https://pubs.usgs.gov/pinatubo/ewert/), [Bautista](https://pubs.usgs.gov/pinatubo/bbautist/). Source files and hashes: ../data/sources.json.']
    (out/'chronology.md').write_text('\n\n'.join(text))
    header=pd.read_csv(Path(header_data_dir or DATA)/'waveform_header_presence.csv',dtype={'file_month':str})
    observed=header[header.file_month.isin(['07','08'])].groupby(['station','channel']).agg(first_july_august_header=('first','min'),last_july_august_header=('last','max'),files=('files','sum')).reset_index()
    observed.to_csv(out/'rebuilt_header_identity_bounds.csv',index=False)
    timing=['# Timing evidence and correction limits','No universal clock correction is applied. Daily WWV checks were intended, but checks were sporadic; nominal ±0.1 s setting accuracy is not an archive accuracy claim.']
    for r in clocks:
        timing.append('## '+r['system'])
        timing.extend('**'+key.replace('_',' ').capitalize()+':** '+str(value) for key,value in r.items() if key!='system')
    (out/'timing_issues.md').write_text('\n\n'.join(timing))
    maps(out);diagrams(out)
    inv=read_inventory(str(out/'pinatubo_provisional.xml'))
    report={'stations':sum(len(n.stations) for n in inv),'channel_epochs':sum(len(s.channels) for n in inv for s in n),'full_count_responses':sum(c.response is not None for n in inv for s in n for c in s),'status':'DRAFT: unknown electronics, undated PI2 low-gain channel, approximate epoch bounds and provisional modern DEM LIL elevation explicit'}
    (out/'validation_summary.json').write_text(json.dumps(report,indent=2));return report
if __name__=='__main__':print(build_all())
