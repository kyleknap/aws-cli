from PyInstaller.utils.hooks import collect_data_files

hiddenimports = ['docutils', 'urllib', 'httplib', 'html.parser',
                 'configparser', 'xml.etree', 'pipes', 'colorama',
                 'awscli.handlers', 'pkg_resources.py2_warn']
datas = collect_data_files('awscli')

