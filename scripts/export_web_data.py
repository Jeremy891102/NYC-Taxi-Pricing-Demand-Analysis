"""Export committed zone-hour data and official TLC geometry for the static app.

Run: pip install pandas pyarrow pyshp shapely
     python scripts/export_web_data.py
No raw trip records are fetched. Official lookup/geometry files are downloaded.
"""
from pathlib import Path
import csv, hashlib, io, json, urllib.request, zipfile
import pandas as pd
import shapefile
from shapely.geometry import shape

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'web/dist/data'
BASE='https://d37ci6vzurychx.cloudfront.net/misc/'

def main():
    source=ROOT/'data/processed/price_elasticity_master.parquet'
    frame=pd.read_parquet(source).sort_values(['PULocationID','pickup_hour'])
    assert not frame.duplicated(['PULocationID','pickup_hour']).any()
    assert frame['pickup_hour'].between(0,23).all()
    lookup_bytes=urllib.request.urlopen(BASE+'taxi_zone_lookup.csv').read()
    lookup={int(r['LocationID']):r for r in csv.DictReader(io.StringIO(lookup_bytes.decode('utf-8-sig')))}
    archive_bytes=urllib.request.urlopen(BASE+'taxi_zones.zip').read()
    archive=zipfile.ZipFile(io.BytesIO(archive_bytes))
    def member(ext): return io.BytesIO(archive.read(next(n for n in archive.namelist() if n.endswith(ext))))
    reader=shapefile.Reader(shp=member('.shp'),shx=member('.shx'),dbf=member('.dbf'))
    entries=[]
    for record in reader.iterShapeRecords():
        info=record.record.as_dict()
        entries.append((int(info['LocationID']),shape(record.shape.__geo_interface__).simplify(75,preserve_topology=True)))
    bounds=[g.bounds for _,g in entries]
    minx=min(b[0] for b in bounds);miny=min(b[1] for b in bounds)
    maxx=max(b[2] for b in bounds);maxy=max(b[3] for b in bounds)
    scale=min(740/(maxx-minx),620/(maxy-miny))
    def ring(coords):
        pts=[f'{20+(x-minx)*scale:.1f},{20+(maxy-y)*scale:.1f}' for x,y,*_ in coords]
        return 'M'+'L'.join(pts)+'Z'
    paths={}
    for zone,g in entries:
        polygons=list(g.geoms) if g.geom_type=='MultiPolygon' else [g]
        paths[zone]=paths.get(zone,'')+''.join(ring(p.exterior.coords)+''.join(ring(h.coords) for h in p.interiors) for p in polygons)
    zones=[{'id':i,'name':v['Zone'],'borough':v['Borough'],'path':paths.get(i,'')} for i,v in sorted(lookup.items())]
    OUT.mkdir(parents=True,exist_ok=True)
    columns=list(frame.columns)
    # pandas JSON conversion maps missing/non-finite values to JSON null.
    rows=json.loads(frame.to_json(orient='values',double_precision=8))
    payload={'columns':columns,'rows':rows,'meta':{'source':'data/processed/price_elasticity_master.parquet','sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'period':'2024','rows':len(frame),'missingHighElasticity':int(frame.elasticity_high.isna().sum()),'unusualPriceRows':int((frame.avg_price>100).sum()),'notes':'Counts are archived zone-hour aggregates averaged over weekday categories, not trips per hour. Revenue is an index, not realized revenue.','lookupSource':BASE+'taxi_zone_lookup.csv','geometrySource':BASE+'taxi_zones.zip','geometrySha256':hashlib.sha256(archive_bytes).hexdigest()}}
    (OUT/'observations.json').write_text(json.dumps(payload,separators=(',',':'),allow_nan=False))
    (OUT/'zones.json').write_text(json.dumps(zones,separators=(',',':')))
    print(f'Exported {len(rows)} observations; {len(zones)} zone labels; {len(paths)} geometries; {payload["meta"]["unusualPriceRows"]} unusual price rows retained.')

if __name__=='__main__': main()
