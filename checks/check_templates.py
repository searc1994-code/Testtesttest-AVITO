from __future__ import annotations

from pathlib import Path
from jinja2 import Environment, FileSystemLoader

templates_dir = Path(__file__).resolve().parents[1] / 'templates'
env = Environment(loader=FileSystemLoader(str(templates_dir)))
count = 0
for path in sorted(templates_dir.glob('*.html')):
    source = path.read_text(encoding='utf-8')
    env.parse(source)
    count += 1
print(f'TEMPLATES_OK {count}')
