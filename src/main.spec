# -*- mode: python ; coding: utf-8 -*-

block_cipher = None


a = Analysis(['main.py'],
             pathex=['D:\\pycharmspace\\HPSS_SDC_TOOL\\src'],
             binaries=[],
              datas=[
                ('./config/*.json', 'config'),
                ('./db/*.dll', 'db')
             ],
             hiddenimports=[
                'workers.hpss_online_model',
                'workers.hpss_proxy_clock',
                'workers.hpss_proxy_robot',
                'workers.hpss_webapi',
		'workers.hpss_report',
		'workers.hpss_cache_manager',
		'tasks.task_test',
		'tasks.task_scada_migrated'
             ],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          [],
          exclude_binaries=True,
          name='main',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          console=True )
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=False,
               upx=True,
               upx_exclude=[],
               name='main')
