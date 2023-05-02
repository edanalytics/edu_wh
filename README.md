# edu_wh

This package creates a dimensional data warehouse to power analytics on data
from the [Ed-Fi data standard](https://www.ed-fi.org/).

It is a highly configurable and extensible package to power analytics on K12 
education data. 

> **Note**
> This package is not complete on its own. Use our [Project Template](https://github.com/edanalytics/edu_project_template) to populate
> necessary configuration.

This package is part of the larger [EDU](https://enabledataunion.org)
analytics framework.

## Installation

dbt version required: `>=1.0.0, <2.0.0`

Include the following in your `packages.yml` file:

```
packages:
  - package: edanalytics/edu_wh
    version: [">=0.2.1", "<0.3.0"]
```

> **Note**
> This package already includes the upstream [Ed-Fi Source package](https://github.com/edanalytics/edu_edfi_source), so you don't need to include it again.

## License
This package is free to use for noncommercial purposes. 
See [License](LICENSE.md).

## Configuration
This package is highly configurable: it needs to parse a variety of [Descriptors](https://techdocs.ed-fi.org/display/EFDS32/Descriptor+Guidance) from Ed-Fi 
and has many settings.

See our [documentation](https://enabledataunion.org/docs/manage_extend/#dbt-configuration) 
for more details on configuration.

## Project Layout

### Ed-Fi Source Data
Processing raw Ed-Fi JSON into nice rectangular tables is handled by the 
[edu_edfi_source](https://github.com/edanalytics/edu_edfi_source). See more 
information on raw data processing there.

### Build
The `build` section of the package does additional pre-processing of data 
from `edu_edfi_source` to prepare it to land in the warehouse. 

When the computations for a warehouse table are too complex to fit into one model,
we break them out into the `build` section. 

### Core Warehouse

`core_warehouse` is where the majority of warehouse tables are defined. 

## Dependencies

This package depends on `edu_edfi_source`, which also imports `dbt_utils`. If
your project also imports these packages, we recommend you remove them from your 
root `packages.yml` to avoid package version conflicts.

## Package Maintenance

The Education Analytics team only maintains the latest version of the package.
We recommend that you stay consistent with the [latest version](https://github.com/edanalytics/edu_wh/releases/latest) of the package
and refer to the [CHANGELOG](https://github.com/edanalytics/edu_wh/blob/main/CHANGELOG.md)
and release notes for more information on changes across versions.

## Platform Compatibility
Currently only Snowflake is supported.

We are working on adding the scaffolding for multi-platform support, and once 
this is in place would welcome contributions.

[Contact us](mailto:edu@edanalytics.org) if you're interested in support in another
platform or contributing to this effort.
