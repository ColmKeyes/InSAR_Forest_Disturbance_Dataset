# InSAR Forest Disturbance Dataset

<!-- Developer-focused header with architecture diagram -->
<div align="center">
  <img src="docs/architecture.png" alt="System Architecture" width="80%">
  <h1>Sentinel-1 InSAR Processing for Forest Disturbance Detection</h1>
  <p>Automated pipeline for generating interferometric coherence stacks for deforestation monitoring</p>
  
  <!-- Technical badges -->
  [![Python 3.10](https://img.shields.io/badge/Python-3.10-blue)](https://python.org)
  [![SNAP Version](https://img.shields.io/badge/SNAP-12.0-orange)](https://step.esa.int)
  [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
  [![DOI](https://img.shields.io/badge/DOI-10.xxxx/xxxxx-blue)](https://doi.org/10.xxxx/xxxxx)
</div>

## :computer: Quick Start
```bash
# Clone repository
git clone https://github.com/ColmKeyes/InSAR_Forest_Disturbance_Dataset.git
cd InSAR_Forest_Disturbance_Dataset

# Install dependencies
conda env create -f environment.yml
conda activate insar-forest

# Run processing pipeline
python bin/1_generate_s1_catalog.py --area borneo
python bin/2_scene_pair_selector.py
python bin/4_download_s1_scenes.py
```

## :gear: API Usage
```python
from src.sentinel1slc import Sentinel1Slc

# Initialize SLC processor
processor = Sentinel1Slc(
    output_dir="data/processed",
    dem_path="data/dem.tif",
    orbit_dir="data/orbits"
)

# Process single scene
processor.process_scene("S1A_IW_SLC_20250101")
```

## :books: Documentation Hub
| Section | Description | Link |
|---------|-------------|------|
| **API Reference** | Full class/method documentation | [API.md](docs/API.md) |
| **Data Schema** | Parquet data schema specification | [SCHEMA.md](docs/SCHEMA.md) |
| **Processing Guide** | Step-by-step pipeline guide | [PROCESSING_GUIDE.md](docs/PROCESSING_GUIDE.md) |
| **Jupyter Examples** | Example notebooks for analysis | [notebooks/](notebooks/) |

## :bar_chart: Data Schema
```mermaid
classDiagram
    class ScenePair {
        +reference_scene: str
        +secondary_scene: str
        +perpendicular_baseline: float
        +temporal_baseline: int
        +coherence: float
    }
    
    class RADDEvent {
        +event_id: UUID
        +timestamp: DateTime
        +geometry: Polygon
        +magnitude: float
        +confidence: float
    }
    
    ScenePair "1" -- "1" RADDEvent : associated_with
```

## :test_tube: Version Compatibility
| Component | Version | Notes |
|-----------|---------|-------|
| Python | 3.10+ | Required |
| SNAP | 12.0 | ESA Sentinel Toolbox |
| GDAL | 3.8+ | Geospatial processing |
| PyroSAR | 0.16+ | SAR processing library |

## :wrench: Processing Pipeline
```mermaid
flowchart LR
    A[Sentinel-1 Catalog] --> B(Scene Pair Selection)
    B --> C(Baseline Calculation)
    C --> D[Download SLCs]
    D --> E[Interferometric Processing]
    E --> F[Coherence Calculation]
    F --> G[Terrain Correction]
    G --> H[Coregistered Stack]
```

## :handshake: Contributing
Contributions are welcome! Please see our [contribution guidelines](CONTRIBUTING.md) for details.

---
*Developed at University College Dublin Earth Institute*
