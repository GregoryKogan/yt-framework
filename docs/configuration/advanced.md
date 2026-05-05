# Advanced configuration

Extra YAML files, OmegaConf features, and how to debug merged config.

## Multiple files

```text
configs/
├── config.yaml
├── config_dev.yaml
├── config_prod.yaml
└── config_large.yaml
```

Pick one at launch:

```bash
python pipeline.py
python pipeline.py --config configs/config_prod.yaml
```

Example split:

`configs/config_dev.yaml`:

```yaml
stages:
  enabled_stages:
    - create_test_data
    - process_data

pipeline:
  mode: "dev"
  build_folder: "//tmp/my_pipeline/dev/build"
```

`configs/config_prod.yaml`:

```yaml
stages:
  enabled_stages:
    - create_input
    - process_data
    - validate_output
    - upload_results

pipeline:
  mode: "prod"
  build_folder: "//home/production/my_pipeline/build"
```

Sample repo layout: [08_multiple_configs](https://github.com/GregoryKogan/yt-framework/tree/main/examples/08_multiple_configs/).

## OmegaConf features

The framework uses OmegaConf. You can lean on:

- **Interpolation** inside YAML:

```yaml
base_path: //tmp/my_pipeline

client:
  input_table: ${base_path}/input
  output_table: ${base_path}/output
```

- **Environment substitution** (OmegaConf 2.x `oc.env` resolver; enable resolvers where you construct config if needed):

```yaml
pipeline:
  build_folder: ${oc.env:BUILD_FOLDER,//tmp/default/build}
```

If you do not use OmegaConf resolvers in your merge path, set `build_folder` in the YAML file you pass to `--config` or export values and generate YAML in CI.

- **Manual merge** in tooling or tests:

```python
from omegaconf import OmegaConf

base = OmegaConf.load("configs/config.yaml")
override = OmegaConf.load("configs/config_prod.yaml")
merged = OmegaConf.merge(base, override)
```

## Validation the framework performs

Expect hard failures when:

- `enabled_stages` missing or empty
- `build_folder` missing while a stage needs upload
- Stage name in YAML does not match a `stages/<name>/` directory
- `config.yaml` missing for an enabled stage
- Malformed YT paths where the client validates them

### Examples

Missing stages list:

```yaml
# bad
stages: {}

# good
stages:
  enabled_stages:
    - my_stage
```

Bad build path (must be absolute YT style with `//` where required):

```yaml
# bad
pipeline:
  build_folder: "tmp/build"

# good
pipeline:
  build_folder: "//tmp/build"
```

## “Inheritance” between pipeline and stage YAML

There is no automatic deep merge of arbitrary keys: the pipeline file supplies `pipeline.*` and the list of stages; each stage file supplies that stage’s subtree. Some operation helpers read `pipeline.build_folder` implicitly—see operation docs for the precise merge rules per operation type.

## Debugging config

Dump what the stage sees:

```python
from omegaconf import OmegaConf

print(OmegaConf.to_yaml(self.config))
print(OmegaConf.to_yaml(self.config.client.operations.map))
```

Guard optional keys:

```python
if "build_folder" in self.deps.pipeline_config.pipeline:
    bf = self.deps.pipeline_config.pipeline.build_folder
```

## See also

- [Configuration index](index.md)
- [Secrets](secrets.md)
- [Troubleshooting: configuration](../troubleshooting/configuration.md)
