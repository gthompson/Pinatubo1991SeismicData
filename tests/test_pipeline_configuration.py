"""Configuration/routing regression tests; no archive processing or FLOVOpy required."""
import ast
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import pinatubo_config as paths
spec=importlib.util.spec_from_file_location('conversion_config_test',ROOT/'10_conversion/pinatubo_steps/config.py')
conversion=importlib.util.module_from_spec(spec);spec.loader.exec_module(conversion)

class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.base=Path(self.temp.name).resolve();self.repo=self.base/'repo';self.repo.mkdir();(self.repo/'.git').mkdir()
        self.inputs=self.base/'original';(self.inputs/'LEGACY').mkdir(parents=True)
        self.out=self.base/'generated'
        self.config=self.repo/'conversion_config.json'
        self.config.write_text(json.dumps(dict(data_top=str(self.inputs),output_root=str(self.out),network='XY',database='TEST')))
    def test_conversion_separates_originals_and_outputs_without_writing(self):
        p=conversion.pipeline_paths(config_file=self.config)
        self.assertEqual(Path(p['SUDS_TOP']),self.inputs/'LEGACY/WAVEFORM_DATA/SUDS')
        self.assertEqual(Path(p['FAIR_TOP']),self.out/'FAIR')
        self.assertEqual(Path(p['TMP_DIR']),self.out/'pipeline_tmp')
        self.assertEqual(Path(p['NETWORK_HISTORY_OUTPUT']),self.out/'network_history')
        self.assertEqual(p['NET'],'XY');self.assertIn('/TEST',p['SEISAN_WAV_DB'])
        self.assertFalse(self.out.exists())
        for key in ['WAVEFORM_INDEX','TRANSLATION_CSV','MERGED_PICK_INDEX','CATALOG_PREFIX','FAIR_REA_DIR','SEISAN_DIAG_DIR']:
            self.assertTrue(Path(p[key]).is_relative_to(self.out),key)
    def test_repository_outputs_rejected(self):
        for value in [self.repo,self.repo/'products',self.repo/'nested/new']:
            with self.assertRaises(ValueError):paths.external_output_root(value,relative_to=self.base,repo_root=self.repo)
    def test_relative_and_symlink_outputs_cannot_bypass_guard(self):
        with self.assertRaises(ValueError):paths.external_output_root('repo/products',relative_to=self.base,repo_root=self.repo)
        alias=self.base/'alias';alias.symlink_to(self.repo,target_is_directory=True)
        with self.assertRaises(ValueError):paths.external_output_root(alias/'products',relative_to=self.base,repo_root=self.repo)
    def test_legacy_inputs_not_overwritten(self):
        for value in [self.inputs/'LEGACY',self.inputs/'LEGACY/nested',self.inputs]:
            with self.assertRaises(ValueError):conversion.pipeline_paths(config_file=self.config,output_root=value)
    def test_analysis_root_and_network_input_are_independent(self):
        cfg=dict(output_root=str(self.out/'analysis'),network_history_products=str(self.out/'conversion/network_history'))
        out=paths.analysis_products_root(cfg,self.repo)
        self.assertEqual(out,self.out/'analysis')
        self.assertEqual(paths.analysis_network_products(cfg,self.repo,out),self.out/'conversion/network_history')
        self.assertFalse(out.exists())
    def test_config_environment_overrides(self):
        with patch.dict(os.environ,{'PINATUBO_CONVERSION_CONFIG':str(self.config),'PINATUBO_CONVERSION_OUTPUT':str(self.out/'other')}):
            self.assertEqual(Path(conversion.pipeline_paths()['OUTPUT_ROOT']),self.out/'other')
        with patch.dict(os.environ,{'PINATUBO_ANALYSIS_OUTPUT':str(self.out/'metrics')}):
            self.assertEqual(paths.analysis_products_root({},self.repo),self.out/'metrics')
    def test_invalid_output_not_silently_accepted(self):
        for bad in ['',False,123,'${PINATUBO_UNDEFINED_VARIABLE}/output']:
            with self.assertRaises(ValueError):paths.external_output_root(bad,relative_to=self.base)
    def test_active_notebooks_use_shared_config_and_have_valid_code(self):
        for p in (ROOT/'10_conversion').glob('*.ipynb'):
            j=json.loads(p.read_text());text='\n'.join(''.join(c['source']) for c in j['cells'] if c['cell_type']=='code')
            self.assertTrue('pipeline_paths(' in text or 'from legacy_bob.runner import run' in text,p.name)
            self.assertNotIn('Dropbox/PROFESSIONAL',text,p.name)
            for c in j['cells']:
                if c['cell_type']=='code':ast.parse(''.join(c['source']))
    def test_analysis_context_routes_manifest_and_caches_externally(self):
        # Exercise the real setup function in isolation from unavailable numerical modules.
        import numpy as np
        import pandas as pd
        source=ROOT/'20_analysis/analysis_workflow.py'
        tree=ast.parse(source.read_text())
        wanted={'load_context','_products','_network_products','_backup_config','configure_selection'}
        subset=ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in wanted],type_ignores=[])
        def digest(v):return hashlib.sha256(v if isinstance(v,bytes) else json.dumps(v,sort_keys=True,default=str).encode()).hexdigest()
        def write_json(p,v):
            p=Path(p);p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(v,default=str))
        ns=dict(Path=Path,json=json,os=os,np=np,pd=pd,__file__=str(source),digest=digest,write_json=write_json,
                WORKFLOW_VERSION='config-test',apply_style=lambda:None,analysis_products_root=paths.analysis_products_root,
                analysis_network_products=paths.analysis_network_products,
                analysis_products=lambda cfg,override=None:Path(override) if override else paths.analysis_products_root(cfg,self.repo),
                runtime_environment=lambda:None,network_products=lambda:self.out/'network_history',
                output_path=lambda stage,*parts,create=False:self.out.joinpath(*parts),
                importlib=SimpleNamespace(import_module=lambda name:SimpleNamespace(__file__=str(source))),shutil=__import__('shutil'))
        def external_path(stage,*parts,create=False):
            p=self.out.joinpath(*parts)
            if create:p.mkdir(parents=True,exist_ok=True)
            return p
        ns['output_path']=external_path
        exec(compile(subset,str(source),'exec'),ns)
        analysis=self.repo/'20_analysis';analysis.mkdir()
        wav=self.inputs/'WAV/1991/05';wav.mkdir(parents=True);(wav/'record').write_bytes(b'not-read-by-setup')
        cfg=dict(wav_root=str(wav.parents[1]),output_root=str(self.out),selection=dict(mode='all',max_files=None,months=['1991/05'],files_per_month=1),processing=dict(minimum_window_s=2,noise_guard_s=1))
        (analysis/'analysis_workflow.json').write_text(json.dumps(cfg))
        ctx=ns['load_context'](analysis)
        self.assertTrue(ctx['out'].is_relative_to(self.out/'notebook_workflow'))
        self.assertTrue((ctx['out']/'run_manifest.json').is_file())
        self.assertEqual(ns['_products'](ctx),self.out)
        self.assertEqual(ns['_network_products'](ctx),self.out/'network_history')
        ns['configure_selection'](analysis,mode='sample',files_per_month=2)
        self.assertTrue(list((self.out/'config_backups').glob('*.json')))
        self.assertFalse((analysis/'products').exists())
        self.assertFalse(list(analysis.glob('*before*')))

if __name__=='__main__':unittest.main()
