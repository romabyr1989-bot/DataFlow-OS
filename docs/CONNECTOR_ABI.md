# Connector Plugin ABI — v1

Export symbol: `dfo_connector_entry` (type `DfoConnector`).

Build: `gcc -shared -fPIC my.c -o my.so`

Required fields: `abi_version=1`, `name`, `create`, `destroy`,
`list_entities`, `describe`, `read_batch`, `ping`.

Optional: `cdc_start`, `cdc_stop`.

Never reorder or remove struct fields. Add new optional fields at end only.
Bump `DFO_CONNECTOR_ABI_VERSION` on breaking changes.
