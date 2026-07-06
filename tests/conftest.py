import os
import sys
from pathlib import Path

os.environ.setdefault('SHOTGUN_TOKEN', 'test-token')
os.environ.setdefault('SHOTGUN_ORGANIZER_ID', '1')

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
