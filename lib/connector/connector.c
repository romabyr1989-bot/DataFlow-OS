#include "connector.h"
#include "../core/log.h"
#include <dlfcn.h>
#include <stdlib.h>
#include <string.h>

struct ConnectorInst {
    void          *dl_handle;
    DfoConnector  *api;
    void          *ctx;
    Arena         *arena;
};

ConnectorInst *connector_load(const char *path, const char *cfg, Arena *a) {
    void *dl = dlopen(path, RTLD_NOW|RTLD_LOCAL);
    if (!dl) { LOG_ERROR("dlopen %s: %s", path, dlerror()); return NULL; }

    DfoConnector *api = dlsym(dl, DFO_CONNECTOR_EXPORT_SYM);
    if (!api) { LOG_ERROR("dlsym %s: %s", DFO_CONNECTOR_EXPORT_SYM, dlerror()); dlclose(dl); return NULL; }

    if (api->abi_version != DFO_CONNECTOR_ABI_VERSION) {
        LOG_ERROR("connector %s ABI version mismatch: got %u want %u",
                  path, api->abi_version, DFO_CONNECTOR_ABI_VERSION);
        dlclose(dl); return NULL;
    }

    void *ctx = api->create(cfg, a);
    if (!ctx) { LOG_ERROR("connector create failed"); dlclose(dl); return NULL; }

    ConnectorInst *inst = malloc(sizeof(ConnectorInst));
    inst->dl_handle = dl; inst->api = api; inst->ctx = ctx; inst->arena = a;
    LOG_INFO("loaded connector %s v%s", api->name, api->version);
    return inst;
}

void connector_unload(ConnectorInst *inst) {
    if (!inst) return;
    if (inst->api->destroy) inst->api->destroy(inst->ctx);
    dlclose(inst->dl_handle);
    free(inst);
}

const DfoConnector *connector_api(ConnectorInst *inst) { return inst->api; }
void               *connector_ctx(ConnectorInst *inst) { return inst->ctx; }
