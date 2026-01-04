日本語版: [README_JP.md](https://github.com/yokoc1322/mitoric/blob/main/README_JP.md)

# mitoric

A library that generates EDA / comparison reports for Polars DataFrames as a single HTML file.

[PyPI](https://pypi.org/project/mitoric/)

## Features

- Generate a report for a single dataset
- Generate a comparison report for two datasets
- Dataset summary (rows/columns/missing/duplicates, etc.)
- Column profiles (numeric/categorical/text/datetime/boolean)
- Correlation/association calculations
- Histogram bin size switching (10/15/30/50)

### Requirements

- Python 3.10+
- polars>=1
- jinja2>=3.1.6

## Usage

### Report for a single dataset

```python
import polars as pl
from mitoric import generate_single_report

frame = pl.DataFrame(
    {
        "age": [10, 12, 12, 14],
        "city": ["A", "B", "A", "C"],
    }
)

html = generate_single_report(frame, save_path="/tmp/report.html")
print(html[:200])
```

### Comparison report for two datasets

```python
import polars as pl
from mitoric import generate_compare_report

left = pl.DataFrame({"value": [1, 2, 3]})
right = pl.DataFrame({"value": [2, 3, 4]})

html = generate_compare_report(
    left,
    right,
    left_name="before",
    right_name="after",
    save_path="/tmp/compare_report.html",
)
```

### Explicit column type specification

```python
import polars as pl
from mitoric import generate_single_report
from mitoric.models.base import ColumnName, ColumnType, ExplicitType

frame = pl.DataFrame({"flag": ["yes", "no", "yes"]})
explicit_types = [ExplicitType(ColumnName("flag"), ColumnType("categorical"))]

html = generate_single_report(frame, explicit_types=explicit_types)
```

## API

- `generate_single_report(frame, *, target_columns=None, explicit_types=None, save_path=None)`
- `generate_compare_report(left, right, *, target_columns=None, explicit_types=None, save_path=None, left_name=None, right_name=None)`

Types supported in `explicit_types`: `numeric`, `categorical`, `text`, `datetime`, `boolean`

## Examples

![Report screenshot](docs/screenshot.png)

- [examples/output/single_report.html](examples/output/single_report.html)
- `examples/output/compare_report.html`

## Constraints and Notes

- Only Polars is supported (Pandas is not supported)
- Generated HTML depends on TailwindCSS and chart.js via CDN
- If `save_path` is not specified, HTML is returned as a string and nothing is saved

## Disclaimer

This library was built with a fairly high ratio of vibe coding, so it may include unnecessary processing, questionable design, or tests. We will improve it as we find issues.

## For Developers

### Setup

```bash
uv sync
```

### Development

```bash
uv sync
make lint
make test
make test_e2e # E2E outputs are saved under examples/output/ for regression checks
```
